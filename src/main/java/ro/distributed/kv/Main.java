package ro.distributed.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.distributed.kv.cluster.FailureDetector;
import ro.distributed.kv.config.ClusterConfig;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Murmur3Hash;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.network.KVClientPool;
import ro.distributed.kv.network.KVServer;
import ro.distributed.kv.network.KVServiceImpl;
import ro.distributed.kv.storage.MemoryStorageEngine;

import java.io.IOException;

/**
 * Punctul principal de intrare pentru pornirea unui nod din clusterul distribuit.
 *
 * Exemplu rulare:
 *   java -cp ... ro.distributed.kv.Main --id=node-1 --port=50051 \
 *        --peers=node-2@localhost:50052,node-3@localhost:50053 [--n=3 --w=2 --r=2]
 *
 * ID-urile peer-ilor trebuie sa coincida cu valorile --id ale celorlalte noduri.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        ClusterConfig config = ClusterConfig.fromArgs(args);
        Node currentNode = config.toCurrentNode();

        log.info("Initializare nod: {} pe portul {}", currentNode.getId(), currentNode.getPort());

        // Inelul de Consistent Hashing
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 150);
        ring.addNode(currentNode);

        // Adaugare noduri partenere (peers) pe inel daca au fost specificate
        for (Node peer : config.getPeers()) {
            ring.addNode(peer);
            log.info("Inregistrat nod partener pe inel: {} ({}:{})",
                    peer.getId(), peer.getHost(), peer.getPort());
        }

        // Motorul de stocare locala
        MemoryStorageEngine storageEngine = new MemoryStorageEngine();

        // Pool conexiuni gRPC
        KVClientPool clientPool = new KVClientPool();

        // Serviciul si serverul gRPC
        FailureDetector failureDetector = new FailureDetector(currentNode, ring, clientPool);
        KVServiceImpl service = new KVServiceImpl(currentNode, ring, storageEngine, clientPool,
                failureDetector,
                config.getReplicationFactor(), config.getWriteQuorum(), config.getReadQuorum(), 2000);
        KVServer server = new KVServer(currentNode.getPort(), service);

        server.start();
        failureDetector.start();
        Runtime.getRuntime().addShutdownHook(new Thread(failureDetector::stop));
        log.info("Nodul {} este activ si gata sa proceseze cereri gRPC!", currentNode.getId());

        server.blockUntilShutdown();
    }
}
