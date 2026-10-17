package ro.distributed.kv.replication;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.distributed.kv.cluster.FailureDetector;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.network.KVClientPool;
import ro.distributed.kv.proto.*;
import ro.distributed.kv.storage.StorageEngine;
import ro.distributed.kv.storage.VersionedValue;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordonatorul Quorum pentru operatiunile PUT si GET distribuite.
 *
 * <p>Implementeaza politica Dynamo-style:
 * <ul>
 *   <li>N = factorul de replicare (implicit 3)</li>
 *   <li>W = numarul minim de confirmari necesare la scriere (implicit 2)</li>
 *   <li>R = numarul minim de confirmari necesare la citire (implicit 2)</li>
 * </ul>
 *
 * <p>Invariant: W + R > N => cel putin o replica va contine mereu valoarea cea mai recenta.
 *
 * <p>Stergerile sunt scrieri de tip tombstone, replicate cu acelasi quorum W; un tombstone
 * castiga prin LWW in fata valorilor mai vechi, deci o stergere nu poate fi "inviata" de replici vechi.
 *
 * <p>Raspunsurile esuate (nod cazut, eroare gRPC) sunt numarate: cand quorum-ul nu mai poate fi
 * atins, operatia esueaza imediat (fail-fast) in loc sa astepte tot timeout-ul. Nodurile marcate
 * DOWN de {@link FailureDetector} sunt sarite fara apel de retea.
 */
public class QuorumCoordinator {

    private static final Logger log = LoggerFactory.getLogger(QuorumCoordinator.class);

    private final Node currentNode;
    private final ConsistentHashRing ring;
    private final StorageEngine localStorageEngine;
    private final KVClientPool clientPool;
    /** Optional: daca e null, toate nodurile sunt considerate vii. */
    private final FailureDetector failureDetector;

    /** Factorul de replicare. */
    private final int n;
    /** Quorum minim pentru scriere (Write). */
    private final int w;
    /** Quorum minim pentru citire (Read). */
    private final int r;
    /** Timeout maxim pentru o operatiune de quorum (ms). */
    private final long quorumTimeoutMs;

    /** Constructor cu valorile implicite Dynamo: N=3, W=2, R=2, timeout=2s. */
    public QuorumCoordinator(Node currentNode,
                             ConsistentHashRing ring,
                             StorageEngine localStorageEngine,
                             KVClientPool clientPool) {
        this(currentNode, ring, localStorageEngine, clientPool, null, 3, 2, 2, 2000);
    }

    public QuorumCoordinator(Node currentNode,
                             ConsistentHashRing ring,
                             StorageEngine localStorageEngine,
                             KVClientPool clientPool,
                             int n, int w, int r, long quorumTimeoutMs) {
        this(currentNode, ring, localStorageEngine, clientPool, null, n, w, r, quorumTimeoutMs);
    }

    /**
     * @param currentNode         nodul curent care actioneaza ca Coordonator
     * @param ring                inelul de Consistent Hashing
     * @param localStorageEngine  motorul de stocare local al acestui nod
     * @param clientPool          pool-ul de conexiuni gRPC catre parteneri
     * @param failureDetector     detectorul de noduri cazute (poate fi null)
     * @param n                   factorul de replicare
     * @param w                   quorum scriere (Write)
     * @param r                   quorum citire (Read)
     * @param quorumTimeoutMs     timeout maxim in milisecunde
     */
    public QuorumCoordinator(Node currentNode,
                             ConsistentHashRing ring,
                             StorageEngine localStorageEngine,
                             KVClientPool clientPool,
                             FailureDetector failureDetector,
                             int n, int w, int r, long quorumTimeoutMs) {
        if (n <= 0 || w <= 0 || r <= 0 || w > n || r > n) {
            throw new IllegalArgumentException(
                    "Parametri quorum invalide: N=" + n + ", W=" + w + ", R=" + r + " (necesar 1 <= W,R <= N)");
        }
        if (w + r <= n) {
            log.warn("W + R <= N ({} + {} <= {}): citirile pot returna date invechite", w, r, n);
        }
        this.currentNode = Objects.requireNonNull(currentNode);
        this.ring = Objects.requireNonNull(ring);
        this.localStorageEngine = Objects.requireNonNull(localStorageEngine);
        this.clientPool = Objects.requireNonNull(clientPool);
        this.failureDetector = failureDetector;
        this.n = n;
        this.w = w;
        this.r = r;
        this.quorumTimeoutMs = quorumTimeoutMs;
    }

    private boolean isAlive(Node node) {
        return failureDetector == null || failureDetector.isAlive(node);
    }

    // =========================================================================
    // Numaratoare de quorum (ack / fail-fast)
    // =========================================================================

    /**
     * Numara confirmarile si esecurile unei operatii pe replici.
     * Se finalizeaza cu {@code true} la atingerea quorum-ului si cu {@code false}
     * de indata ce quorum-ul nu mai poate fi atins.
     */
    private static final class QuorumTracker {
        private final int required;
        private final int total;
        private final AtomicInteger acks = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();
        private final List<String> ackedNodes = Collections.synchronizedList(new ArrayList<>());
        private final CompletableFuture<Boolean> done = new CompletableFuture<>();

        QuorumTracker(int required, int total) {
            this.required = required;
            this.total = total;
        }

        void ack(String nodeId) {
            ackedNodes.add(nodeId); // inainte de increment: lista e completa cand quorum-ul e semnalat
            if (acks.incrementAndGet() >= required) {
                done.complete(true);
            }
        }

        void fail() {
            if (failures.incrementAndGet() > total - required) {
                done.complete(false);
            }
        }

        /** @return true doar daca quorum-ul a fost atins in timp util. */
        boolean await(long timeoutMs) throws InterruptedException {
            try {
                return done.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return false;
            } catch (ExecutionException e) {
                return false;
            }
        }

        int acks() { return acks.get(); }
        List<String> ackedNodes() { return new ArrayList<>(ackedNodes); }
    }

    // =========================================================================
    // QUORUM WRITE (PUT / DELETE)
    // =========================================================================

    /** PUT cu quorum. Timestamp-ul si versiunea sunt generate o singura data de coordonator. */
    public WriteResult quorumPut(String key, byte[] valueBytes, long timestamp, long version) {
        return quorumWrite(key, new VersionedValue(valueBytes, timestamp, version));
    }

    /** DELETE cu quorum: scrie un tombstone pe replici, cu aceleasi garantii ca un PUT. */
    public WriteResult quorumDelete(String key, long timestamp, long version) {
        return quorumWrite(key, VersionedValue.tombstone(timestamp, version));
    }

    /**
     * Calculeaza preference list (primele N noduri fizice unice), trimite paralel scrierea
     * si asteapta W confirmari sau esec/timeout.
     */
    private WriteResult quorumWrite(String key, VersionedValue value) {
        List<Node> preferenceList = ring.getPreferenceList(key, n);

        if (preferenceList.isEmpty()) {
            return WriteResult.failure("Inelul este gol - niciun nod disponibil");
        }

        String op = value.isTombstone() ? "DELETE" : "PUT";
        int total = preferenceList.size();
        int effectiveW = Math.min(w, total);
        log.info("[QUORUM {}] key={} | replici={} | W={}", op, key, total, effectiveW);

        QuorumTracker tracker = new QuorumTracker(effectiveW, total);
        InternalPutRequest internalReq = toInternalPut(key, value);

        for (Node replica : preferenceList) {
            if (!isAlive(replica)) {
                log.warn("[QUORUM {}] Replica {} este DOWN - sarita", op, replica.getId());
                tracker.fail();
            } else if (replica.equals(currentNode)) {
                try {
                    localStorageEngine.putIfNewer(key, value);
                    tracker.ack(currentNode.getId());
                } catch (Exception e) {
                    log.warn("[QUORUM {}] Eroare la scriere locala: {}", op, e.getMessage());
                    tracker.fail();
                }
            } else {
                sendAsync(replica, internalReq, op, tracker);
            }
        }

        boolean achieved;
        try {
            achieved = tracker.await(quorumTimeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return WriteResult.failure("Operatiunea a fost intrerupta");
        }

        int totalAcks = tracker.acks();
        List<String> confirmedNodes = tracker.ackedNodes();

        if (achieved) {
            log.info("[QUORUM {}] SUCCES: key={} | ack-uri={}/{} | noduri={}", op, key, totalAcks, total, confirmedNodes);
            return WriteResult.success(totalAcks, total, confirmedNodes);
        }
        log.warn("[QUORUM {}] ESEC QUORUM: key={} | ack-uri={}/{} (necesar W={})", op, key, totalAcks, total, effectiveW);
        return WriteResult.quorumTimeout(totalAcks, total, confirmedNodes, effectiveW);
    }

    private void sendAsync(Node replica, InternalPutRequest req, String op, QuorumTracker tracker) {
        ListenableFuture<InternalPutResponse> future;
        try {
            future = clientPool.getFutureStub(replica).internalPut(req);
        } catch (Exception e) {
            log.warn("[QUORUM {}] Replica {} nu poate fi contactata: {}", op, replica.getId(), e.getMessage());
            tracker.fail();
            return;
        }
        future.addListener(() -> {
            try {
                InternalPutResponse resp = future.get();
                if (resp.getSuccess()) {
                    tracker.ack(replica.getId());
                } else {
                    log.warn("[QUORUM {}] Replica {} a returnat esec", op, replica.getId());
                    tracker.fail();
                }
            } catch (Exception e) {
                log.warn("[QUORUM {}] Replica {} nu a raspuns: {}", op, replica.getId(), rootMessage(e));
                tracker.fail();
            }
        }, Runnable::run);
    }

    // =========================================================================
    // QUORUM READ (GET)
    // =========================================================================

    /**
     * GET cu quorum: interogheaza toate replicile, asteapta R raspunsuri, aplica Last-Write-Wins
     * (timestamp, apoi versiune). Un tombstone castigator inseamna NOT_FOUND. Replicile cu date
     * invechite sunt reparate asincron (Read Repair).
     */
    public ReadResult quorumGet(String key) {
        List<Node> preferenceList = ring.getPreferenceList(key, n);

        if (preferenceList.isEmpty()) {
            return ReadResult.failure("Inelul este gol - niciun nod disponibil");
        }

        int total = preferenceList.size();
        int effectiveR = Math.min(r, total);
        log.info("[QUORUM GET] key={} | replici={} | R={}", key, total, effectiveR);

        QuorumTracker tracker = new QuorumTracker(effectiveR, total);
        // nod -> valoarea returnata (inclusiv tombstone); nodurile fara cheie nu apar aici
        Map<String, VersionedValue> perNodeValues = new ConcurrentHashMap<>();
        InternalGetRequest req = InternalGetRequest.newBuilder().setKey(key).build();

        for (Node replica : preferenceList) {
            final String replicaId = replica.getId();
            if (!isAlive(replica)) {
                log.warn("[QUORUM GET] Replica {} este DOWN - sarita", replicaId);
                tracker.fail();
            } else if (replica.equals(currentNode)) {
                try {
                    localStorageEngine.get(key).ifPresent(v -> perNodeValues.put(replicaId, v));
                    tracker.ack(replicaId);
                } catch (Exception e) {
                    log.warn("[QUORUM GET] Eroare la citire locala: {}", e.getMessage());
                    tracker.fail();
                }
            } else {
                ListenableFuture<InternalGetResponse> future;
                try {
                    future = clientPool.getFutureStub(replica).internalGet(req);
                } catch (Exception e) {
                    log.warn("[QUORUM GET] Replica {} nu poate fi contactata: {}", replicaId, e.getMessage());
                    tracker.fail();
                    continue;
                }
                future.addListener(() -> {
                    try {
                        InternalGetResponse resp = future.get();
                        if (resp.getFound()) {
                            perNodeValues.put(replicaId, new VersionedValue(
                                    resp.getValue().toByteArray(),
                                    resp.getTimestamp(),
                                    resp.getVersion(),
                                    resp.getTombstone()));
                        }
                        tracker.ack(replicaId);
                    } catch (Exception e) {
                        log.warn("[QUORUM GET] Replica {} nu a raspuns: {}", replicaId, rootMessage(e));
                        tracker.fail();
                    }
                }, Runnable::run);
            }
        }

        boolean achieved;
        try {
            achieved = tracker.await(quorumTimeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ReadResult.failure("Operatiunea a fost intrerupta");
        }

        int responses = tracker.acks();
        if (!achieved) {
            log.warn("[QUORUM GET] ESEC QUORUM: key={} | raspunsuri={} (necesar R={})", key, responses, effectiveR);
            return ReadResult.quorumTimeout(responses, effectiveR);
        }

        Map<String, VersionedValue> snapshot = new LinkedHashMap<>(perNodeValues);
        Optional<VersionedValue> winner = snapshot.values().stream().max(VersionedValue::compareLww);

        if (winner.isEmpty()) {
            log.info("[QUORUM GET] Cheia {} nu a fost gasita ({} raspunsuri)", key, responses);
            return ReadResult.notFound(responses);
        }

        VersionedValue winnerValue = winner.get();
        triggerReadRepairAsync(key, winnerValue, snapshot, preferenceList);

        if (winnerValue.isTombstone()) {
            log.info("[QUORUM GET] Cheia {} este stearsa (tombstone ts={})", key, winnerValue.getTimestamp());
            return ReadResult.notFound(responses);
        }

        log.info("[QUORUM GET] SUCCES LWW: key={} | ts={} | raspunsuri={}", key, winnerValue.getTimestamp(), responses);
        return ReadResult.found(winnerValue, responses);
    }

    // =========================================================================
    // READ REPAIR (async)
    // =========================================================================

    /**
     * Trimite valoarea LWW (inclusiv tombstone) replicilor vii care au date mai vechi sau lipsa.
     * Nu blocheaza raspunsul catre client; aplicarea pe replica se face cu putIfNewer.
     */
    private void triggerReadRepairAsync(String key,
                                        VersionedValue latest,
                                        Map<String, VersionedValue> perNodeValues,
                                        List<Node> allReplicas) {
        InternalPutRequest repairReq = toInternalPut(key, latest);

        for (Node replica : allReplicas) {
            if (!isAlive(replica)) {
                continue;
            }
            VersionedValue nodeValue = perNodeValues.get(replica.getId());
            boolean needsRepair = nodeValue == null
                    ? !latest.isTombstone()          // un tombstone nu trebuie impins pe noduri fara cheie
                    : latest.isNewerThan(nodeValue);
            if (!needsRepair) {
                continue;
            }

            log.info("[READ REPAIR] key={} -> {} (ts local={}, ts winner={})",
                    key, replica.getId(),
                    nodeValue != null ? nodeValue.getTimestamp() : "lipsa",
                    latest.getTimestamp());

            if (replica.equals(currentNode)) {
                try {
                    localStorageEngine.putIfNewer(key, latest);
                } catch (Exception e) {
                    log.warn("[READ REPAIR] Eroare la repararea locala a cheii {}: {}", key, e.getMessage());
                }
            } else {
                try {
                    ListenableFuture<InternalPutResponse> f = clientPool.getFutureStub(replica).internalPut(repairReq);
                    f.addListener(() -> {
                        try {
                            f.get();
                        } catch (Exception e) {
                            log.warn("[READ REPAIR] Replica {} nu a putut fi reparata: {}", replica.getId(), rootMessage(e));
                        }
                    }, Runnable::run);
                } catch (Exception e) {
                    log.warn("[READ REPAIR] Replica {} nu poate fi contactata: {}", replica.getId(), e.getMessage());
                }
            }
        }
    }

    // =========================================================================
    // Utilitare
    // =========================================================================

    private static InternalPutRequest toInternalPut(String key, VersionedValue value) {
        return InternalPutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value.getData()))
                .setTimestamp(value.getTimestamp())
                .setVersion(value.getVersion())
                .setTombstone(value.isTombstone())
                .build();
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getMessage();
    }

    // =========================================================================
    // Getteri configurare
    // =========================================================================

    public int getN() { return n; }
    public int getW() { return w; }
    public int getR() { return r; }
    public long getQuorumTimeoutMs() { return quorumTimeoutMs; }
}
