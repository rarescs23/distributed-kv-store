package ro.distributed.kv.network;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.distributed.kv.cluster.FailureDetector;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.proto.*;
import ro.distributed.kv.replication.QuorumCoordinator;
import ro.distributed.kv.replication.ReadResult;
import ro.distributed.kv.replication.WriteResult;
import ro.distributed.kv.storage.StorageEngine;
import ro.distributed.kv.storage.VersionedValue;

import java.util.Objects;
import java.util.Optional;

/**
 * Implementarea serviciului gRPC KeyValueService.
 *
 * <p><b>Faza 2</b>: Coordonator / Proxy transparent pentru cererile ale caror chei
 * apartin altor noduri conform inelului.
 *
 * <p><b>Faza 3</b>: Coordonatorul executa scrieri si citiri cu <em>Quorum</em>
 * (N=3, W=2, R=2) prin {@link QuorumCoordinator}, replicand datele pe N succesori
 * unici din preference list si aplicand Last-Write-Wins la citire.
 * Stergerile sunt replicate ca tombstone-uri.
 */
public class KVServiceImpl extends KeyValueServiceGrpc.KeyValueServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private final Node currentNode;
    private final ConsistentHashRing ring;
    private final StorageEngine storageEngine;
    private final KVClientPool clientPool;
    private final QuorumCoordinator quorumCoordinator;

    /**
     * Constructor principal folosit in productie.
     * QuorumCoordinator este creat cu valorile implicite N=3, W=2, R=2.
     */
    public KVServiceImpl(Node currentNode,
                         ConsistentHashRing ring,
                         StorageEngine storageEngine,
                         KVClientPool clientPool) {
        this.currentNode = Objects.requireNonNull(currentNode, "Nodul curent nu poate fi null");
        this.ring = Objects.requireNonNull(ring, "Inelul nu poate fi null");
        this.storageEngine = Objects.requireNonNull(storageEngine, "StorageEngine nu poate fi null");
        this.clientPool = Objects.requireNonNull(clientPool, "ClientPool nu poate fi null");
        this.quorumCoordinator = new QuorumCoordinator(currentNode, ring, storageEngine, clientPool);
    }

    /**
     * Constructor cu detector de noduri cazute si parametri de quorum configurabili.
     */
    public KVServiceImpl(Node currentNode,
                         ConsistentHashRing ring,
                         StorageEngine storageEngine,
                         KVClientPool clientPool,
                         FailureDetector failureDetector,
                         int n, int w, int r, long quorumTimeoutMs) {
        this(currentNode, ring, storageEngine, clientPool,
                new QuorumCoordinator(currentNode, ring, storageEngine, clientPool,
                        failureDetector, n, w, r, quorumTimeoutMs));
    }

    /**
     * Constructor avansat pentru testare sau configurare custom a quorum-ului.
     */
    public KVServiceImpl(Node currentNode,
                         ConsistentHashRing ring,
                         StorageEngine storageEngine,
                         KVClientPool clientPool,
                         QuorumCoordinator quorumCoordinator) {
        this.currentNode = Objects.requireNonNull(currentNode, "Nodul curent nu poate fi null");
        this.ring = Objects.requireNonNull(ring, "Inelul nu poate fi null");
        this.storageEngine = Objects.requireNonNull(storageEngine, "StorageEngine nu poate fi null");
        this.clientPool = Objects.requireNonNull(clientPool, "ClientPool nu poate fi null");
        this.quorumCoordinator = Objects.requireNonNull(quorumCoordinator, "QuorumCoordinator nu poate fi null");
    }

    // =========================================================================
    // 1. API PUBLIC CLIENT – operatiuni coordonate cu Quorum (Faza 3)
    // =========================================================================

    /**
     * Versiunea urmatoare pentru o cheie: versiunea locala cunoscuta + 1 (1 pentru chei noi).
     * Timestamp-ul ramane criteriul principal LWW; versiunea desparte scrierile cu timestamp egal.
     */
    private long nextVersion(String key) {
        return storageEngine.get(key).map(v -> v.getVersion() + 1).orElse(1L);
    }

    /**
     * PUT public: orice nod care primeste aceasta cerere devine Coordonator.
     * Replicate pe N noduri, raspunde clientului dupa W confirmari (Quorum Write).
     */
    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String key = request.getKey();
        byte[] valueBytes = request.getValue().toByteArray();

        // Timestamp si version sunt generate O SINGURA DATA de Coordonator,
        // pentru a asigura ca toate replicile primesc exact aceleasi metadate LWW.
        long timestamp = System.currentTimeMillis();
        long version = nextVersion(key);

        log.info("[PUT] Nodul {} coordoneaza PUT pentru cheia {} (N={}, W={})",
                currentNode.getId(), key,
                quorumCoordinator.getN(), quorumCoordinator.getW());

        try {
            WriteResult result = quorumCoordinator.quorumPut(key, valueBytes, timestamp, version);

            if (result.isSuccess()) {
                PutResponse response = PutResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage(String.format(
                                "Quorum atins: %d/%d replici au confirmat. Noduri: %s",
                                result.getAcksReceived(),
                                result.getTotalReplicas(),
                                result.getConfirmedNodes()))
                        .setStoredOnNode(String.join(",", result.getConfirmedNodes()))
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } else if (result.getStatus() == WriteResult.Status.QUORUM_TIMEOUT) {
                log.warn("[PUT] Quorum esuat pentru cheia {}: {}/{} ack-uri (necesar W={})",
                        key, result.getAcksReceived(), result.getTotalReplicas(),
                        quorumCoordinator.getW());
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription(String.format(
                                "QUORUM_TIMEOUT: doar %d/%d replici au confirmat (necesar W=%d)",
                                result.getAcksReceived(), result.getTotalReplicas(),
                                quorumCoordinator.getW()))
                        .asRuntimeException());

            } else {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Eroare interna la PUT: " + result.getErrorMessage())
                        .asRuntimeException());
            }

        } catch (Exception e) {
            log.error("[PUT] Exceptie neasteptata pentru cheia {}: {}", key, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Eroare interna: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    /**
     * GET public: orice nod care primeste aceasta cerere devine Coordonator.
     * Citeste din R replici, aplica Last-Write-Wins, declanseaza Read Repair asincron.
     */
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        String key = request.getKey();

        log.info("[GET] Nodul {} coordoneaza GET pentru cheia {} (N={}, R={})",
                currentNode.getId(), key,
                quorumCoordinator.getN(), quorumCoordinator.getR());

        try {
            ReadResult result = quorumCoordinator.quorumGet(key);

            if (result.isFound()) {
                VersionedValue val = result.getValue().orElseThrow();
                GetResponse response = GetResponse.newBuilder()
                        .setFound(true)
                        .setValue(ByteString.copyFrom(val.getData()))
                        .setTimestamp(val.getTimestamp())
                        .setVersion(val.getVersion())
                        .setServedByNode(currentNode.getId() + " (quorum:" + result.getResponsesReceived() + ")")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } else if (result.getStatus() == ReadResult.Status.NOT_FOUND) {
                GetResponse response = GetResponse.newBuilder()
                        .setFound(false)
                        .setServedByNode(currentNode.getId())
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } else if (result.getStatus() == ReadResult.Status.QUORUM_TIMEOUT) {
                log.warn("[GET] Quorum esuat pentru cheia {}: {}/{} raspunsuri (necesar R={})",
                        key, result.getResponsesReceived(), quorumCoordinator.getN(),
                        quorumCoordinator.getR());
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription(String.format(
                                "QUORUM_TIMEOUT: doar %d raspunsuri primite (necesar R=%d)",
                                result.getResponsesReceived(), quorumCoordinator.getR()))
                        .asRuntimeException());

            } else {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Eroare interna la GET: " + result.getErrorMessage())
                        .asRuntimeException());
            }

        } catch (Exception e) {
            log.error("[GET] Exceptie neasteptata pentru cheia {}: {}", key, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Eroare interna: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    /**
     * DELETE public: orice nod care primeste cererea devine Coordonator si scrie un
     * <em>tombstone</em> pe replici, cu acelasi quorum W ca un PUT. Tombstone-ul castiga prin LWW
     * in fata valorilor mai vechi, deci cheia stearsa nu poate reapare din replici invechite.
     */
    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String key = request.getKey();
        long timestamp = System.currentTimeMillis();
        long version = nextVersion(key);

        log.info("[DELETE] Nodul {} coordoneaza DELETE pentru cheia {} (N={}, W={})",
                currentNode.getId(), key, quorumCoordinator.getN(), quorumCoordinator.getW());

        try {
            WriteResult result = quorumCoordinator.quorumDelete(key, timestamp, version);

            if (result.isSuccess()) {
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage(String.format(
                                "Quorum atins: %d/%d replici au confirmat stergerea. Noduri: %s",
                                result.getAcksReceived(), result.getTotalReplicas(), result.getConfirmedNodes()))
                        .build());
                responseObserver.onCompleted();
            } else if (result.getStatus() == WriteResult.Status.QUORUM_TIMEOUT) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription(String.format(
                                "QUORUM_TIMEOUT: doar %d/%d replici au confirmat stergerea (necesar W=%d)",
                                result.getAcksReceived(), result.getTotalReplicas(), quorumCoordinator.getW()))
                        .asRuntimeException());
            } else {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Eroare interna la DELETE: " + result.getErrorMessage())
                        .asRuntimeException());
            }
        } catch (Exception e) {
            log.error("[DELETE] Exceptie neasteptata pentru cheia {}: {}", key, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Eroare interna: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    // =========================================================================
    // 2. API INTERN DE CLUSTER – executie directa pe storage local, fara re-rutare
    // =========================================================================

    /**
     * Salveaza direct pe storage-ul local al acestui nod.
     * Apelat de Coordonator in cadrul Quorum Write sau Read Repair.
     * NU consulta inelul - evita bucle infinite de forwarding.
     */
    @Override
    public void internalPut(InternalPutRequest request, StreamObserver<InternalPutResponse> responseObserver) {
        String key = request.getKey();
        byte[] data = request.getValue().toByteArray();
        long timestamp = request.getTimestamp() > 0 ? request.getTimestamp() : System.currentTimeMillis();
        long version = request.getVersion() > 0 ? request.getVersion() : 1L;

        log.debug("[INTERNAL PUT] Nodul {} salveaza direct cheia {}", currentNode.getId(), key);

        // putIfNewer: o scriere/repair intarziat nu poate suprascrie o valoare mai noua (LWW)
        boolean applied = storageEngine.putIfNewer(key,
                new VersionedValue(data, timestamp, version, request.getTombstone()));

        InternalPutResponse response = InternalPutResponse.newBuilder()
                .setSuccess(true)
                .setMessage((applied ? "Salvat cu succes pe nodul " : "Ignorat (exista o versiune mai noua) pe nodul ")
                        + currentNode.getId())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Citeste direct de pe storage-ul local al acestui nod.
     * Apelat de Coordonator in cadrul Quorum Read.
     */
    @Override
    public void internalGet(InternalGetRequest request, StreamObserver<InternalGetResponse> responseObserver) {
        String key = request.getKey();
        log.debug("[INTERNAL GET] Nodul {} citeste direct cheia {}", currentNode.getId(), key);

        Optional<VersionedValue> valOpt = storageEngine.get(key);

        if (valOpt.isPresent()) {
            VersionedValue val = valOpt.get();
            InternalGetResponse response = InternalGetResponse.newBuilder()
                    .setFound(true)
                    .setValue(ByteString.copyFrom(val.getData()))
                    .setTimestamp(val.getTimestamp())
                    .setVersion(val.getVersion())
                    .setTombstone(val.isTombstone())
                    .build();
            responseObserver.onNext(response);
        } else {
            responseObserver.onNext(InternalGetResponse.newBuilder().setFound(false).build());
        }
        responseObserver.onCompleted();
    }

    /**
     * Sterge direct de pe storage-ul local al acestui nod.
     */
    @Override
    public void internalDelete(InternalDeleteRequest request, StreamObserver<InternalDeleteResponse> responseObserver) {
        boolean deleted = storageEngine.delete(request.getKey());
        InternalDeleteResponse response = InternalDeleteResponse.newBuilder()
                .setSuccess(deleted)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Heartbeat – raspunde imediat confirmare de viata.
     */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .setReceiverId(currentNode.getId())
                .setActive(true)
                .setTimestamp(System.currentTimeMillis())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
