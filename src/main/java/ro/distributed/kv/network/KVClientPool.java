package ro.distributed.kv.network;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.proto.KeyValueServiceGrpc;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Pool thread-safe pentru conexiunile gRPC catre nodurile partenere din cluster.
 * Reutilizeaza canalele gRPC (ManagedChannel) pentru a evita costul deschiderii
 * repetate a conexiunilor TCP.
 */
public class KVClientPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KVClientPool.class);

    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final long defaultTimeoutMs;

    public KVClientPool() {
        this(3000); // 3 secunde timeout implicit
    }

    public KVClientPool(long defaultTimeoutMs) {
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    /**
     * Obtine sau creeaza un ManagedChannel reutilizabil catre un nod.
     * Suporta atat retea TCP reala, cat si canale in-process pentru teste automate.
     */
    public ManagedChannel getChannel(Node node) {
        Objects.requireNonNull(node, "Nodul nu poate fi null");
        return channels.computeIfAbsent(node.getId(), id -> {
            if ("in-process".equalsIgnoreCase(node.getHost())) {
                log.info("Creare canal gRPC in-process catre nodul {}", node.getId());
                return io.grpc.inprocess.InProcessChannelBuilder.forName(node.getId())
                        .usePlaintext()
                        .build();
            } else {
                log.info("Creare canal gRPC reutilizabil catre nodul {} ({}:{})",
                        node.getId(), node.getHost(), node.getPort());
                return ManagedChannelBuilder.forAddress(node.getHost(), node.getPort())
                        .usePlaintext() // Comunicare interna in plaintext
                        .build();
            }
        });
    }

    /**
     * Obtine un BlockingStub cu timeout configurat pentru operatiuni sincrone.
     */
    public KeyValueServiceGrpc.KeyValueServiceBlockingStub getBlockingStub(Node node) {
        return KeyValueServiceGrpc.newBlockingStub(getChannel(node))
                .withDeadlineAfter(defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Obtine un FutureStub pentru operatiuni asincrone (folosit si la Quorum in Faza 3).
     */
    public KeyValueServiceGrpc.KeyValueServiceFutureStub getFutureStub(Node node) {
        return KeyValueServiceGrpc.newFutureStub(getChannel(node))
                .withDeadlineAfter(defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Inchide un canal specific catre un nod (de exemplu cand nodul devine inactiv).
     */
    public void closeChannel(String nodeId) {
        ManagedChannel channel = channels.remove(nodeId);
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        log.info("Inchidere pool conexiuni gRPC ({} canale active)", channels.size());
        for (ManagedChannel channel : channels.values()) {
            if (!channel.isShutdown()) {
                try {
                    channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    channel.shutdownNow();
                }
            }
        }
        channels.clear();
    }
}
