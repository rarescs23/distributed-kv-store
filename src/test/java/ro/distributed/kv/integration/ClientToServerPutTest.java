package ro.distributed.kv.integration;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.*;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Murmur3Hash;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.network.KVClientPool;
import ro.distributed.kv.network.KVServer;
import ro.distributed.kv.network.KVServiceImpl;
import ro.distributed.kv.proto.*;
import ro.distributed.kv.storage.MemoryStorageEngine;
import ro.distributed.kv.storage.VersionedValue;

import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de integrare minimal care simuleaza scenariul:
 *
 *   Client --> PUT("user:42", "Alice") --> Node 1 (Coordonator)
 *                                              |
 *                                       Scriere Locala
 *                                              |
 *                               InternalPut -> Node 2 (Replica)
 *
 * Foloseste gRPC In-Process (fara TCP real, fara porturi deschise).
 */
class ClientToServerPutTest {

    // ──────────────────────────────────────────────────────────────────────
    // Componentele clusterului simulat
    // ──────────────────────────────────────────────────────────────────────

    private Node node1;
    private Node node2;

    private MemoryStorageEngine storageNode1;
    private MemoryStorageEngine storageNode2;

    private KVServer server1;
    private KVServer server2;

    private ManagedChannel clientChannel;
    private KeyValueServiceGrpc.KeyValueServiceBlockingStub clientStub;

    @BeforeEach
    void setUp() throws IOException {
        // 1. Definim cele doua noduri fizice
        //    Porturile sunt fictive (doar identificatori) - transportul e in-process prin RAM
        node1 = new Node("node-1", "in-process", 50251);
        node2 = new Node("node-2", "in-process", 50252);

        // 2. Fiecare nod are propriul storage independent in RAM
        storageNode1 = new MemoryStorageEngine();
        storageNode2 = new MemoryStorageEngine();

        // 3. Inelul de hashing comun - ambele noduri il cunosc
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 150);
        ring.addNode(node1);
        ring.addNode(node2);

        // 4. Asamblam Node 2 (replica) - pool gol, nu are replici proprii in test
        KVClientPool poolNode2 = new KVClientPool();
        KVServiceImpl serviceNode2 = new KVServiceImpl(node2, ring, storageNode2, poolNode2);
        server2 = new KVServer("node-2", serviceNode2);
        server2.start();

        // 5. Asamblam Node 1 (coordonator) - pool-ul lui stie de node-2
        KVClientPool poolNode1 = new KVClientPool();
        KVServiceImpl serviceNode1 = new KVServiceImpl(node1, ring, storageNode1, poolNode1);
        server1 = new KVServer("node-1", serviceNode1);
        server1.start();

        // 6. Clientul extern se conecteaza la Node 1 prin canal in-process
        clientChannel = InProcessChannelBuilder.forName("node-1")
                .directExecutor()
                .build();

        clientStub = KeyValueServiceGrpc.newBlockingStub(clientChannel);
    }

    @AfterEach
    void tearDown() {
        if (clientChannel != null) clientChannel.shutdownNow();
        if (server1 != null) server1.stop();
        if (server2 != null) server2.stop();
    }

    // ──────────────────────────────────────────────────────────────────────
    // SCENARIUL 1: PUT reusit - raspunsul confirma succesul
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("PUT reusit: serverul returneaza success=true")
    void put_returneazaSuccessTrue() {
        PutResponse raspuns = clientStub.put(PutRequest.newBuilder()
                .setKey("user:42")
                .setValue(ByteString.copyFromUtf8("{\"name\": \"Alice\"}"))
                .build());

        assertThat(raspuns.getSuccess())
                .as("PUT trebuie sa returneze success=true")
                .isTrue();
    }

    // ──────────────────────────────────────────────────────────────────────
    // SCENARIUL 2: Datele exista fizic in storage-ul nodului responsabil
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("PUT reusit: datele exista in storage-ul nodului responsabil dupa scriere")
    void put_dateleExistaInStorageDupaScriere() {
        clientStub.put(PutRequest.newBuilder()
                .setKey("user:42")
                .setValue(ByteString.copyFromUtf8("{\"name\": \"Alice\"}"))
                .build());

        // Cel putin unul dintre noduri trebuie sa detina cheia
        boolean existaUndeva = storageNode1.containsKey("user:42")
                || storageNode2.containsKey("user:42");

        assertThat(existaUndeva)
                .as("Dupa PUT, cheia trebuie sa existe in storage-ul cel putin al unui nod")
                .isTrue();
    }

    // ──────────────────────────────────────────────────────────────────────
    // SCENARIUL 3: Continutul salvat este exact cel trimis de client
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("PUT reusit: continutul stocat este identic cu cel trimis de client")
    void put_continutulStoratEsteCorect() {
        String valoareTrimisa = "{\"name\": \"Alice\"}";

        clientStub.put(PutRequest.newBuilder()
                .setKey("user:42")
                .setValue(ByteString.copyFromUtf8(valoareTrimisa))
                .build());

        // Cautam valoarea in ambele storage-uri (oricare nod poate fi responsabil)
        Optional<VersionedValue> valoare = storageNode1.containsKey("user:42")
                ? storageNode1.get("user:42")
                : storageNode2.get("user:42");

        assertThat(valoare)
                .as("Valoarea trebuie sa fie prezenta in cluster")
                .isPresent();

        assertThat(valoare.get().getDataAsString())
                .as("Continutul salvat trebuie sa fie identic cu cel trimis de client")
                .isEqualTo(valoareTrimisa);
    }

    // ──────────────────────────────────────────────────────────────────────
    // SCENARIUL 4: VersionedValue are metadata valida (timestamp generat)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("PUT reusit: VersionedValue are timestamp valid generat la momentul scrierii")
    void put_timestampEsteValid() {
        long inainte = System.currentTimeMillis();

        clientStub.put(PutRequest.newBuilder()
                .setKey("produs:99")
                .setValue(ByteString.copyFromUtf8("Laptop"))
                .build());

        long dupa = System.currentTimeMillis();

        Optional<VersionedValue> valoare = storageNode1.containsKey("produs:99")
                ? storageNode1.get("produs:99")
                : storageNode2.get("produs:99");

        assertThat(valoare).isPresent();
        assertThat(valoare.get().getTimestamp())
                .as("Timestamp-ul trebuie sa fie generat in intervalul executiei testului")
                .isBetween(inainte, dupa);
    }

    // ──────────────────────────────────────────────────────────────────────
    // SCENARIUL 5: A doua scriere pe aceeasi cheie suprascrie prima (LWW)
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("PUT de doua ori: Last-Write-Wins - a doua valoare suprascrie prima")
    void put_lastWriteWins_aDouaValoareSuprascrie() throws InterruptedException {
        // Prima scriere
        clientStub.put(PutRequest.newBuilder()
                .setKey("user:1")
                .setValue(ByteString.copyFromUtf8("Alice"))
                .build());

        // Mica pauza ca timestamp-urile sa fie distincte
        Thread.sleep(5);

        // A doua scriere pe aceeasi cheie
        clientStub.put(PutRequest.newBuilder()
                .setKey("user:1")
                .setValue(ByteString.copyFromUtf8("Alice-Updated"))
                .build());

        Optional<VersionedValue> valoare = storageNode1.containsKey("user:1")
                ? storageNode1.get("user:1")
                : storageNode2.get("user:1");

        assertThat(valoare).isPresent();
        assertThat(valoare.get().getDataAsString())
                .as("A doua valoare trebuie sa o suprascrie pe prima (Last-Write-Wins)")
                .isEqualTo("Alice-Updated");
    }

    // ──────────────────────────────────────────────────────────────────────
    // SCENARIUL 6: GET dupa PUT returneaza valoarea corecta
    // ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("GET dupa PUT: clientul citeste exact valoarea scrisa anterior")
    void get_dupaPut_returneazaValoareaCorecta() {
        String cheie = "session:abc123";
        String valoare = "{\"userId\": 7, \"role\": \"admin\"}";

        // Scriem valoarea
        clientStub.put(PutRequest.newBuilder()
                .setKey(cheie)
                .setValue(ByteString.copyFromUtf8(valoare))
                .build());

        // Citim valoarea inapoi de la acelasi nod
        GetResponse raspuns = clientStub.get(GetRequest.newBuilder()
                .setKey(cheie)
                .build());

        assertThat(raspuns.getFound())
                .as("GET trebuie sa gaseasca cheia scrisa anterior")
                .isTrue();

        assertThat(raspuns.getValue().toStringUtf8())
                .as("Valoarea citita trebuie sa fie identica cu cea scrisa")
                .isEqualTo(valoare);
    }
}
