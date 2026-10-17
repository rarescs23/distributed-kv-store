package ro.distributed.kv.network;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Murmur3Hash;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.proto.*;
import ro.distributed.kv.storage.MemoryStorageEngine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KVNetworkIntegrationTest {

    private static final int PORT_NODE_1 = 50151;
    private static final int PORT_NODE_2 = 50152;

    private Node node1;
    private Node node2;
    private ConsistentHashRing ring;

    private MemoryStorageEngine storage1;
    private MemoryStorageEngine storage2;

    private KVClientPool clientPool1;
    private KVClientPool clientPool2;

    private KVServer server1;
    private KVServer server2;

    private ManagedChannel clientChannel1;
    private ManagedChannel clientChannel2;
    private KeyValueServiceGrpc.KeyValueServiceBlockingStub clientStub1;
    private KeyValueServiceGrpc.KeyValueServiceBlockingStub clientStub2;

    @BeforeEach
    void setUp() throws IOException {
        node1 = new Node("node-1", "in-process", PORT_NODE_1);
        node2 = new Node("node-2", "in-process", PORT_NODE_2);

        ring = new ConsistentHashRing(new Murmur3Hash(), 100);
        ring.addNode(node1);
        ring.addNode(node2);

        storage1 = new MemoryStorageEngine();
        storage2 = new MemoryStorageEngine();

        clientPool1 = new KVClientPool();
        clientPool2 = new KVClientPool();

        KVServiceImpl service1 = new KVServiceImpl(node1, ring, storage1, clientPool1);
        KVServiceImpl service2 = new KVServiceImpl(node2, ring, storage2, clientPool2);

        server1 = new KVServer("node-1", service1);
        server2 = new KVServer("node-2", service2);

        server1.start();
        server2.start();

        // Canale gRPC pentru clientul de test catre noduri
        clientChannel1 = io.grpc.inprocess.InProcessChannelBuilder.forName("node-1")
                .usePlaintext()
                .build();
        clientChannel2 = io.grpc.inprocess.InProcessChannelBuilder.forName("node-2")
                .usePlaintext()
                .build();

        clientStub1 = KeyValueServiceGrpc.newBlockingStub(clientChannel1);
        clientStub2 = KeyValueServiceGrpc.newBlockingStub(clientChannel2);
    }

    @AfterEach
    void tearDown() {
        if (clientChannel1 != null) clientChannel1.shutdownNow();
        if (clientChannel2 != null) clientChannel2.shutdownNow();

        if (server1 != null) server1.stop();
        if (server2 != null) server2.stop();

        if (clientPool1 != null) clientPool1.close();
        if (clientPool2 != null) clientPool2.close();
    }

    private String findKeyResponsibleFor(Node targetNode) {
        while (true) {
            String candidate = "key-" + UUID.randomUUID().toString().substring(0, 8);
            if (targetNode.equals(ring.getNode(candidate))) {
                return candidate;
            }
        }
    }

    @Test
    @DisplayName("Quorum Write + Read: PUT replicat pe ambele noduri, GET returneaza valoarea LWW")
    void shouldProcessLocallyWhenTargetIsCurrentNode() {
        String keyForNode1 = findKeyResponsibleFor(node1);
        ByteString value = ByteString.copyFrom("valoare-locala-nod-1", StandardCharsets.UTF_8);

        // Clientul trimite PUT catre Node 1 (va actiona ca Coordonator Quorum)
        PutResponse putResp = clientStub1.put(PutRequest.newBuilder()
                .setKey(keyForNode1)
                .setValue(value)
                .build());

        // Cu Quorum (N=2, W=2) intr-un cluster de 2 noduri, ambele noduri trebuie sa confirme
        assertThat(putResp.getSuccess()).isTrue();
        assertThat(putResp.getStoredOnNode()).contains("node-1");

        // Cheia trebuie sa fie prezenta pe node-1 (in preference list)
        assertThat(storage1.containsKey(keyForNode1)).isTrue();
        assertThat(storage1.get(keyForNode1).get().getDataAsString()).isEqualTo("valoare-locala-nod-1");

        // Clientul citeste cu Quorum de la Node 1 (coordonator)
        GetResponse getResp = clientStub1.get(GetRequest.newBuilder()
                .setKey(keyForNode1)
                .build());

        assertThat(getResp.getFound()).isTrue();
        assertThat(getResp.getValue().toStringUtf8()).isEqualTo("valoare-locala-nod-1");
        // served_by_node contine id-ul coordonatorului cu sufixul quorum
        assertThat(getResp.getServedByNode()).startsWith("node-1");
    }

    @Test
    @DisplayName("Quorum Write (PUT): Node 1 ca Coordonator repliceaza pe preference list (include Node 2)")
    void shouldForwardPutTransparentlyToResponsibleNode() {
        // Gasim o cheie a carei responsabilitate principala apartine Node 2
        String keyForNode2 = findKeyResponsibleFor(node2);
        ByteString value = ByteString.copyFrom("date-salvate-pe-node-2", StandardCharsets.UTF_8);

        // Trimitem cererea catre NODE 1 – el devine Coordonator Quorum
        PutResponse putResp = clientStub1.put(PutRequest.newBuilder()
                .setKey(keyForNode2)
                .setValue(value)
                .build());

        // Quorum atins – scrierea a reusit
        assertThat(putResp.getSuccess()).isTrue();
        // Preference list (N=2) contine ambele noduri – ambele apar in stored_on_node
        assertThat(putResp.getStoredOnNode()).contains("node-2");

        // Node 2 (nodul primar) detine cheia
        assertThat(storage2.containsKey(keyForNode2)).isTrue();
        assertThat(storage2.get(keyForNode2).get().getDataAsString()).isEqualTo("date-salvate-pe-node-2");
    }

    @Test
    @DisplayName("Quorum Read (GET): Node 1 ca Coordonator citeste din R replici si returneaza valoarea LWW")
    void shouldForwardGetTransparentlyToResponsibleNode() {
        String keyForNode2 = findKeyResponsibleFor(node2);
        ByteString value = ByteString.copyFrom("valoare-secreta-pe-nod2", StandardCharsets.UTF_8);

        // Scriere initiala prin Node 2 (va replica quorum pe ambele noduri)
        clientStub2.put(PutRequest.newBuilder()
                .setKey(keyForNode2)
                .setValue(value)
                .build());

        // Clientul interogheaza NODE 1 cu Quorum Read
        GetResponse getResp = clientStub1.get(GetRequest.newBuilder()
                .setKey(keyForNode2)
                .build());

        // Quorum Read a colectat R raspunsuri si a ales valoarea LWW
        assertThat(getResp.getFound()).isTrue();
        assertThat(getResp.getValue().toStringUtf8()).isEqualTo("valoare-secreta-pe-nod2");
        // served_by_node contine id-ul coordonatorului (node-1)
        assertThat(getResp.getServedByNode()).startsWith("node-1");
    }

    @Test
    @DisplayName("Quorum DELETE: scrie tombstone pe replici, iar GET ulterior nu mai gaseste cheia")
    void shouldDeleteWithQuorumUsingTombstone() {
        String keyForNode2 = findKeyResponsibleFor(node2);
        storage2.put(keyForNode2, ro.distributed.kv.storage.VersionedValue.of("de-sters", 1L, 1L));

        // Clientul trimite DELETE catre Node 1 (coordonator)
        DeleteResponse delResp = clientStub1.delete(DeleteRequest.newBuilder()
                .setKey(keyForNode2)
                .build());

        assertThat(delResp.getSuccess()).isTrue();
        // Valoarea nu este eliminata fizic, ci inlocuita cu un tombstone mai nou
        assertThat(storage2.get(keyForNode2)).isPresent();
        assertThat(storage2.get(keyForNode2).get().isTombstone()).isTrue();
        assertThat(storage1.get(keyForNode2).get().isTombstone()).isTrue();

        GetResponse getResp = clientStub1.get(GetRequest.newBuilder().setKey(keyForNode2).build());
        assertThat(getResp.getFound()).isFalse();
    }

    @Test
    @DisplayName("Heartbeat: Nodurile raspund activ la apelul de heartbeat inter-noduri")
    void shouldRespondToHeartbeat() {
        HeartbeatResponse resp = clientStub1.heartbeat(HeartbeatRequest.newBuilder()
                .setSenderId("monitor")
                .setTimestamp(System.currentTimeMillis())
                .build());

        assertThat(resp.getReceiverId()).isEqualTo("node-1");
        assertThat(resp.getActive()).isTrue();
    }
}
