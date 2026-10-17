package ro.distributed.kv.integration;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ro.distributed.kv.cluster.FailureDetector;
import ro.distributed.kv.config.ClusterConfig;
import ro.distributed.kv.core.ConsistentHashRing;
import ro.distributed.kv.core.Murmur3Hash;
import ro.distributed.kv.core.Node;
import ro.distributed.kv.network.KVClientPool;
import ro.distributed.kv.network.KVServer;
import ro.distributed.kv.network.KVServiceImpl;
import ro.distributed.kv.proto.*;
import ro.distributed.kv.storage.MemoryStorageEngine;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster de 3 noduri pe sockets TCP reale (localhost), configurat ca in {@code Main}:
 * fiecare nod isi construieste inelul din argumentele {@code --id} / {@code --peers=id@host:port}.
 */
class TcpClusterTest {

    private final List<KVServer> servers = new ArrayList<>();
    private final List<KVClientPool> pools = new ArrayList<>();
    private final List<FailureDetector> detectors = new ArrayList<>();
    private ManagedChannel clientChannel;
    private KeyValueServiceGrpc.KeyValueServiceBlockingStub client;

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        int[] ports = {freePort(), freePort(), freePort()};
        String[] ids = {"node-1", "node-2", "node-3"};

        for (int i = 0; i < 3; i++) {
            List<String> args = new ArrayList<>();
            args.add("--id=" + ids[i]);
            args.add("--port=" + ports[i]);
            StringBuilder peers = new StringBuilder("--peers=");
            for (int j = 0; j < 3; j++) {
                if (j != i) {
                    if (peers.length() > "--peers=".length()) peers.append(',');
                    peers.append(ids[j]).append("@127.0.0.1:").append(ports[j]);
                }
            }
            args.add(peers.toString());
            ClusterConfig config = ClusterConfig.fromArgs(args.toArray(new String[0]));

            Node self = config.toCurrentNode();
            ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 150);
            ring.addNode(self);
            config.getPeers().forEach(ring::addNode);

            KVClientPool pool = new KVClientPool();
            FailureDetector detector = new FailureDetector(self, ring, pool, 100, 300, 2);
            KVServiceImpl service = new KVServiceImpl(self, ring, new MemoryStorageEngine(), pool, detector,
                    config.getReplicationFactor(), config.getWriteQuorum(), config.getReadQuorum(), 2000);
            KVServer server = new KVServer(config.getPort(), service);
            server.start();
            detector.start();

            pools.add(pool);
            detectors.add(detector);
            servers.add(server);
        }

        clientChannel = ManagedChannelBuilder.forAddress("127.0.0.1", ports[0]).usePlaintext().build();
        client = KeyValueServiceGrpc.newBlockingStub(clientChannel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        clientChannel.shutdownNow();
        detectors.forEach(FailureDetector::stop);
        servers.forEach(KVServer::stop);
        pools.forEach(KVClientPool::close);
    }

    @Test
    @DisplayName("TCP real: PUT / GET / DELETE prin orice nod, inclusiv cu un nod oprit")
    void shouldServeRequestsOverRealSockets() throws Exception {
        PutResponse put = client.put(PutRequest.newBuilder()
                .setKey("user:42").setValue(ByteString.copyFromUtf8("Ion")).build());
        assertThat(put.getSuccess()).isTrue();

        GetResponse get = client.get(GetRequest.newBuilder().setKey("user:42").build());
        assertThat(get.getFound()).isTrue();
        assertThat(get.getValue().toStringUtf8()).isEqualTo("Ion");

        // opreste nodul 3; clusterul (N=3, W=2, R=2) trebuie sa mearga in continuare
        servers.get(2).stop();

        assertThat(client.put(PutRequest.newBuilder()
                .setKey("user:43").setValue(ByteString.copyFromUtf8("Maria")).build()).getSuccess()).isTrue();
        assertThat(client.get(GetRequest.newBuilder().setKey("user:43").build())
                .getValue().toStringUtf8()).isEqualTo("Maria");

        assertThat(client.delete(DeleteRequest.newBuilder().setKey("user:42").build()).getSuccess()).isTrue();
        assertThat(client.get(GetRequest.newBuilder().setKey("user:42").build()).getFound()).isFalse();
    }
}
