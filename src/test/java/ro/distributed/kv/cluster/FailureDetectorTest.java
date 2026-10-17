package ro.distributed.kv.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Murmur3Hash;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.network.KVClientPool;
import ro.distributed.kv.network.KVServer;
import ro.distributed.kv.network.KVServiceImpl;
import ro.distributed.kv.storage.MemoryStorageEngine;

import java.io.IOException;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class FailureDetectorTest {

    private Node node1;
    private Node node2;
    private KVClientPool pool1;
    private KVClientPool pool2;
    private KVServer server1;
    private KVServer server2;
    private KVServiceImpl service2;
    private FailureDetector detector;

    @BeforeEach
    void setUp() throws IOException {
        node1 = new Node("fd-node-1", "in-process", 50261);
        node2 = new Node("fd-node-2", "in-process", 50262);

        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 20);
        ring.addNode(node1);
        ring.addNode(node2);

        pool1 = new KVClientPool();
        pool2 = new KVClientPool();
        server1 = new KVServer(node1.getId(),
                new KVServiceImpl(node1, ring, new MemoryStorageEngine(), pool1));
        service2 = new KVServiceImpl(node2, ring, new MemoryStorageEngine(), pool2);
        server2 = new KVServer(node2.getId(), service2);
        server1.start();
        server2.start();

        detector = new FailureDetector(node1, ring, pool1, 50, 200, 2);
    }

    @AfterEach
    void tearDown() {
        detector.stop();
        server1.stop();
        server2.stop();
        pool1.close();
        pool2.close();
    }

    private static boolean eventually(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }

    @Test
    @DisplayName("Nodurile necunoscute si nodul curent sunt considerate vii")
    void unknownAndSelfAreAlive() {
        assertThat(detector.isAlive(node1)).isTrue();
        assertThat(detector.isAlive(node2)).isTrue();
    }

    @Test
    @DisplayName("Un singur esec nu declara nodul DOWN; pragul de esecuri consecutive o face")
    void shouldMarkDownOnlyAfterThreshold() {
        server2.stop();

        detector.checkAll();
        assertThat(detector.isAlive(node2)).isTrue();   // 1 esec < prag 2

        detector.checkAll();
        assertThat(detector.isAlive(node2)).isFalse();  // 2 esecuri consecutive
    }

    @Test
    @DisplayName("Un heartbeat reusit intrerupe seria de esecuri")
    void successShouldResetFailureStreak() {
        detector.recordFailure(node2);
        detector.recordSuccess(node2);
        detector.recordFailure(node2);
        assertThat(detector.isAlive(node2)).isTrue();
    }

    @Test
    @DisplayName("Detectorul periodic detecteaza caderea si revenirea unui nod")
    void shouldDetectFailureAndRecoveryAutomatically() throws Exception {
        detector.start();
        assertThat(detector.isAlive(node2)).isTrue();

        server2.stop();
        assertThat(eventually(() -> !detector.isAlive(node2), 5000)).as("node-2 marcat DOWN").isTrue();

        server2 = new KVServer(node2.getId(), service2);
        server2.start();
        assertThat(eventually(() -> detector.isAlive(node2), 10000)).as("node-2 revenit UP").isTrue();
    }
}
