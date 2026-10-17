package ro.distributed.kv.replication;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ro.distributed.kv.cluster.FailureDetector;
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
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cluster in-process de 3 noduri cu N=3, W=2, R=2: fiecare cheie este replicata pe toate nodurile.
 */
class QuorumClusterTest {

    private static final String KEY = "user:42";

    private final Node[] nodes = new Node[3];
    private final MemoryStorageEngine[] storages = new MemoryStorageEngine[3];
    private final KVClientPool[] pools = new KVClientPool[3];
    private final KVServiceImpl[] services = new KVServiceImpl[3];
    private final KVServer[] servers = new KVServer[3];
    private final ManagedChannel[] channels = new ManagedChannel[3];
    private final KeyValueServiceGrpc.KeyValueServiceBlockingStub[] stubs =
            new KeyValueServiceGrpc.KeyValueServiceBlockingStub[3];
    private FailureDetector detector0;

    @BeforeEach
    void setUp() throws IOException {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 50);
        for (int i = 0; i < 3; i++) {
            nodes[i] = new Node("q-node-" + (i + 1), "in-process", 50351 + i);
            ring.addNode(nodes[i]);
        }
        for (int i = 0; i < 3; i++) {
            storages[i] = new MemoryStorageEngine();
            pools[i] = new KVClientPool();
        }
        // nodul 0 foloseste un detector rapid, ca sa putem verifica fail-fast pe noduri DOWN
        detector0 = new FailureDetector(nodes[0], ring, pools[0], 50, 200, 2);
        for (int i = 0; i < 3; i++) {
            services[i] = new KVServiceImpl(nodes[i], ring, storages[i], pools[i],
                    i == 0 ? detector0 : null, 3, 2, 2, 2000);
            servers[i] = new KVServer(nodes[i].getId(), services[i]);
            servers[i].start();
            channels[i] = InProcessChannelBuilder.forName(nodes[i].getId()).usePlaintext().build();
            stubs[i] = KeyValueServiceGrpc.newBlockingStub(channels[i]);
        }
    }

    @AfterEach
    void tearDown() {
        detector0.stop();
        for (int i = 0; i < 3; i++) {
            channels[i].shutdownNow();
            servers[i].stop();
            pools[i].close();
        }
    }

    private void stopNode(int i) {
        servers[i].stop();
    }

    private void restartNode(int i) throws IOException {
        servers[i] = new KVServer(nodes[i].getId(), services[i]);
        servers[i].start();
    }

    private PutResponse put(int coordinator, String key, String value) {
        return stubs[coordinator].put(PutRequest.newBuilder()
                .setKey(key).setValue(ByteString.copyFromUtf8(value)).build());
    }

    private GetResponse get(int coordinator, String key) {
        return stubs[coordinator].get(GetRequest.newBuilder().setKey(key).build());
    }

    private DeleteResponse delete(int coordinator, String key) {
        return stubs[coordinator].delete(DeleteRequest.newBuilder().setKey(key).build());
    }

    private static boolean eventually(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            Thread.sleep(25);
        }
        return condition.getAsBoolean();
    }

    @Test
    @DisplayName("PUT cu toate nodurile vii: cheia este replicata pe toate cele 3 noduri")
    void putShouldReplicateToAllNodes() {
        assertThat(put(0, KEY, "Ion").getSuccess()).isTrue();

        for (int i = 0; i < 3; i++) {
            assertThat(storages[i].get(KEY)).as("replica " + i).isPresent();
            assertThat(storages[i].get(KEY).get().getDataAsString()).isEqualTo("Ion");
        }
        GetResponse resp = get(1, KEY);
        assertThat(resp.getFound()).isTrue();
        assertThat(resp.getValue().toStringUtf8()).isEqualTo("Ion");
    }

    @Test
    @DisplayName("Un nod cazut: PUT si GET reusesc rapid (W=2, R=2 din 3), fara sa astepte timeout-ul")
    void shouldTolerateSingleNodeFailure() {
        stopNode(2);

        long start = System.currentTimeMillis();
        assertThat(put(0, KEY, "Maria").getSuccess()).isTrue();
        GetResponse resp = get(1, KEY);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(resp.getFound()).isTrue();
        assertThat(resp.getValue().toStringUtf8()).isEqualTo("Maria");
        assertThat(storages[2].containsKey(KEY)).isFalse();
        assertThat(elapsed).as("fail-fast, nu timeout de 2s").isLessThan(1500);
    }

    @Test
    @DisplayName("Doua noduri cazute: quorum imposibil, PUT/GET/DELETE esueaza imediat cu UNAVAILABLE")
    void shouldFailFastWhenQuorumIsImpossible() {
        stopNode(1);
        stopNode(2);

        long start = System.currentTimeMillis();
        assertThatThrownBy(() -> put(0, KEY, "x"))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
        assertThatThrownBy(() -> get(0, KEY)).isInstanceOf(StatusRuntimeException.class);
        assertThatThrownBy(() -> delete(0, KEY)).isInstanceOf(StatusRuntimeException.class);
        assertThat(System.currentTimeMillis() - start).isLessThan(1500);
    }

    @Test
    @DisplayName("Nodul marcat DOWN de FailureDetector este sarit fara apel de retea")
    void shouldSkipNodesMarkedDownByFailureDetector() throws Exception {
        detector0.start();
        stopNode(2);
        assertThat(eventually(() -> !detector0.isAlive(nodes[2]), 5000)).isTrue();

        assertThat(put(0, KEY, "v").getSuccess()).isTrue();
        assertThat(get(0, KEY).getFound()).isTrue();
    }

    @Test
    @DisplayName("DELETE cu quorum: GET intoarce not found, iar replicile retin tombstone")
    void deleteShouldWriteTombstonesAndHideKey() {
        put(0, KEY, "Ion");
        assertThat(delete(1, KEY).getSuccess()).isTrue();

        for (int i = 0; i < 3; i++) {
            assertThat(storages[i].get(KEY).get().isTombstone()).as("replica " + i).isTrue();
        }
        assertThat(get(2, KEY).getFound()).isFalse();
    }

    @Test
    @DisplayName("O replica cazuta in timpul DELETE nu poate 'invia' cheia dupa revenire; read repair o corecteaza")
    void staleReplicaMustNotResurrectDeletedKey() throws Exception {
        put(0, KEY, "Ion");
        stopNode(2);
        assertThat(delete(0, KEY).getSuccess()).isTrue();
        assertThat(storages[2].get(KEY).get().isTombstone()).isFalse(); // inca are valoarea veche

        restartNode(2);

        // nodul 2 coordoneaza: citeste local valoarea veche + un raspuns remote cu tombstone
        assertThat(get(2, KEY).getFound()).as("cheia stearsa nu trebuie sa reapara").isFalse();
        assertThat(eventually(() -> storages[2].get(KEY).get().isTombstone(), 2000))
                .as("read repair aplica tombstone-ul pe replica veche").isTrue();
    }

    @Test
    @DisplayName("Read repair: o replica cu valoare veche este actualizata la citire")
    void readRepairShouldHealStaleReplica() throws Exception {
        long now = System.currentTimeMillis();
        storages[0].put(KEY, VersionedValue.of("nou", now, 2L));
        storages[1].put(KEY, VersionedValue.of("nou", now, 2L));
        storages[2].put(KEY, VersionedValue.of("vechi", now - 10_000, 1L));

        GetResponse resp = get(0, KEY);
        assertThat(resp.getValue().toStringUtf8()).isEqualTo("nou");

        assertThat(eventually(() -> storages[2].get(KEY).get().getDataAsString().equals("nou"), 2000)).isTrue();
    }

    @Test
    @DisplayName("LWW: o scriere intarziata (mai veche) nu suprascrie o valoare mai noua")
    void staleInternalPutMustNotOverwriteNewerValue() {
        long now = System.currentTimeMillis();
        storages[0].put(KEY, VersionedValue.of("nou", now, 2L));

        InternalPutResponse resp = stubs[0].internalPut(InternalPutRequest.newBuilder()
                .setKey(KEY)
                .setValue(ByteString.copyFromUtf8("vechi"))
                .setTimestamp(now - 5000)
                .setVersion(1)
                .build());

        assertThat(resp.getSuccess()).isTrue();
        assertThat(storages[0].get(KEY).get().getDataAsString()).isEqualTo("nou");
    }

    @Test
    @DisplayName("Scrieri succesive: versiunea creste, iar GET returneaza ultima valoare")
    void successiveWritesShouldIncreaseVersion() {
        put(0, KEY, "v1");
        put(0, KEY, "v2");
        put(1, KEY, "v3");

        GetResponse resp = get(2, KEY);
        assertThat(resp.getValue().toStringUtf8()).isEqualTo("v3");
        assertThat(resp.getVersion()).isGreaterThanOrEqualTo(2L);
    }

    @Test
    @DisplayName("Parametri de quorum invalizi sunt respinsi")
    void shouldRejectInvalidQuorumParameters() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 5);
        assertThatThrownBy(() -> new QuorumCoordinator(nodes[0], ring, storages[0], pools[0], 3, 4, 2, 1000))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
