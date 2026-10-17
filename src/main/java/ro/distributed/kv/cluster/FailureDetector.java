package ro.distributed.kv.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.network.KVClientPool;
import ro.distributed.kv.proto.HeartbeatRequest;
import ro.distributed.kv.proto.HeartbeatResponse;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Detector de noduri cazute bazat pe heartbeat.
 *
 * <p>La fiecare {@code intervalMs}, trimite un RPC {@code Heartbeat} catre toate nodurile
 * de pe inel (mai putin nodul curent). Un nod este declarat <b>DOWN</b> dupa
 * {@code failureThreshold} esecuri consecutive si revine <b>UP</b> la primul heartbeat reusit.
 * Coordonatorul de quorum foloseste starea pentru a nu astepta timeout-uri pe noduri cazute.
 *
 * <p>Thread-safe. Nodurile necunoscute sunt considerate vii (optimist).
 */
public class FailureDetector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FailureDetector.class);

    private final Node currentNode;
    private final ConsistentHashRing ring;
    private final KVClientPool clientPool;
    private final long intervalMs;
    private final long heartbeatTimeoutMs;
    private final int failureThreshold;

    private final ConcurrentMap<String, AtomicInteger> consecutiveFailures = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> down = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;

    public FailureDetector(Node currentNode, ConsistentHashRing ring, KVClientPool clientPool) {
        this(currentNode, ring, clientPool, 1000, 500, 3);
    }

    public FailureDetector(Node currentNode,
                           ConsistentHashRing ring,
                           KVClientPool clientPool,
                           long intervalMs,
                           long heartbeatTimeoutMs,
                           int failureThreshold) {
        this.currentNode = Objects.requireNonNull(currentNode);
        this.ring = Objects.requireNonNull(ring);
        this.clientPool = Objects.requireNonNull(clientPool);
        if (intervalMs <= 0 || heartbeatTimeoutMs <= 0 || failureThreshold <= 0) {
            throw new IllegalArgumentException("Parametrii detectorului trebuie sa fie pozitivi");
        }
        this.intervalMs = intervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.failureThreshold = failureThreshold;
    }

    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "failure-detector-" + currentNode.getId());
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::checkAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("[FAILURE DETECTOR] Pornit pe {} (interval={}ms, prag={} esecuri)",
                currentNode.getId(), intervalMs, failureThreshold);
    }

    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public void close() {
        stop();
    }

    /** Un singur ciclu de verificare (public pentru teste deterministe). */
    public void checkAll() {
        for (Node node : ring.getPhysicalNodes()) {
            if (node.equals(currentNode)) {
                continue;
            }
            try {
                checkNode(node);
            } catch (RuntimeException e) {
                log.warn("[FAILURE DETECTOR] Eroare neasteptata la verificarea {}: {}", node.getId(), e.getMessage());
            }
        }
    }

    private void checkNode(Node node) {
        try {
            HeartbeatResponse resp = clientPool.getBlockingStub(node)
                    .withDeadlineAfter(heartbeatTimeoutMs, TimeUnit.MILLISECONDS)
                    .heartbeat(HeartbeatRequest.newBuilder()
                            .setSenderId(currentNode.getId())
                            .setTimestamp(System.currentTimeMillis())
                            .build());
            if (resp.getActive()) {
                recordSuccess(node);
            } else {
                recordFailure(node);
            }
        } catch (Exception e) {
            recordFailure(node);
        }
    }

    /** Inregistreaza un heartbeat reusit; nodul devine UP daca era DOWN. */
    public void recordSuccess(Node node) {
        consecutiveFailures.computeIfAbsent(node.getId(), id -> new AtomicInteger()).set(0);
        if (down.remove(node.getId()) != null) {
            log.info("[FAILURE DETECTOR] Nodul {} a REVENIT (UP)", node.getId());
        }
    }

    /** Inregistreaza un esec; nodul devine DOWN la atingerea pragului. */
    public void recordFailure(Node node) {
        int failures = consecutiveFailures.computeIfAbsent(node.getId(), id -> new AtomicInteger())
                .incrementAndGet();
        if (failures >= failureThreshold && down.putIfAbsent(node.getId(), Boolean.TRUE) == null) {
            log.warn("[FAILURE DETECTOR] Nodul {} este considerat DOWN ({} esecuri consecutive)",
                    node.getId(), failures);
        }
    }

    public boolean isAlive(Node node) {
        return node.equals(currentNode) || !down.containsKey(node.getId());
    }
}
