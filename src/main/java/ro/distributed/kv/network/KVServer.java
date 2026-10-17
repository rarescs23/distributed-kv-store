package ro.distributed.kv.network;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Gestioneaza ciclul de viata al serverului gRPC (pornire, ascultare pe port, oprire ordonata).
 */
public class KVServer {

    private static final Logger log = LoggerFactory.getLogger(KVServer.class);

    private final String inProcessName;
    private final int port;
    private final KVServiceImpl service;
    private Server server;

    public KVServer(int port, KVServiceImpl service) {
        this.port = port;
        this.inProcessName = null;
        this.service = Objects.requireNonNull(service, "KVServiceImpl nu poate fi null");
    }

    public KVServer(String inProcessName, KVServiceImpl service) {
        this.port = -1;
        this.inProcessName = Objects.requireNonNull(inProcessName, "inProcessName nu poate fi null");
        this.service = Objects.requireNonNull(service, "KVServiceImpl nu poate fi null");
    }

    /**
     * Porneste serverul gRPC (pe port TCP sau in-process).
     */
    public synchronized void start() throws IOException {
        if (server != null && !server.isShutdown()) {
            log.warn("Serverul gRPC ruleaza deja");
            return;
        }

        if (inProcessName != null) {
            server = io.grpc.inprocess.InProcessServerBuilder.forName(inProcessName)
                    .addService(service)
                    .directExecutor()
                    .build()
                    .start();
            log.info("Serverul gRPC in-process '{}' a pornit cu succes", inProcessName);
        } else {
            server = ServerBuilder.forPort(port)
                    .addService(service)
                    .build()
                    .start();
            log.info("Serverul gRPC a pornit cu succes pe portul {}", server.getPort());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook declansat pentru serverul gRPC");
            KVServer.this.stop();
        }));
    }

    /**
     * Opreste serverul gRPC ordonat (graceful shutdown).
     */
    public synchronized void stop() {
        if (server != null) {
            log.info("Oprire server gRPC pe portul {}", port);
            server.shutdown();
            try {
                if (!server.awaitTermination(2, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
            server = null;
        }
    }

    /**
     * Blocheaza firul curent de executie pana la oprirea serverului (pentru daemon / CLI).
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Returneaza portul efectiv al serverului (foarte util in teste daca folosim port 0).
     */
    public int getPort() {
        return server != null ? server.getPort() : port;
    }

    /**
     * Verifica daca serverul ruleaza activ.
     */
    public boolean isRunning() {
        return server != null && !server.isShutdown() && !server.isTerminated();
    }
}
