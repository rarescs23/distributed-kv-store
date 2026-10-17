package ro.distributed.kv.config;

import ro.distributed.kv.core.Node;

import java.util.*;

/**
 * Configurarea unui nod din cluster (port gRPC, ID si lista de noduri partenere / seeds).
 */
public class ClusterConfig {

    private final String nodeId;
    private final String host;
    private final int port;
    private final List<Node> peers;
    private final int replicationFactor;
    private final int writeQuorum;
    private final int readQuorum;

    public ClusterConfig(String nodeId, String host, int port, List<Node> peers) {
        this(nodeId, host, port, peers, 3, 2, 2);
    }

    public ClusterConfig(String nodeId, String host, int port, List<Node> peers,
                         int replicationFactor, int writeQuorum, int readQuorum) {
        this.replicationFactor = replicationFactor;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId nu poate fi null");
        this.host = Objects.requireNonNull(host, "host nu poate fi null");
        this.port = port;
        this.peers = peers != null ? new ArrayList<>(peers) : Collections.emptyList();
    }

    public static ClusterConfig fromArgs(String[] args) {
        String id = "node-" + UUID.randomUUID().toString().substring(0, 6);
        String host = "127.0.0.1";
        int port = 50051;
        List<Node> peers = new ArrayList<>();
        int n = 3, w = 2, r = 2;

        for (String arg : args) {
            if (arg.startsWith("--id=")) {
                id = arg.substring(5).trim();
            } else if (arg.startsWith("--host=")) {
                host = arg.substring(7).trim();
            } else if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring(7).trim());
            } else if (arg.startsWith("--n=")) {
                n = Integer.parseInt(arg.substring(4).trim());
            } else if (arg.startsWith("--w=")) {
                w = Integer.parseInt(arg.substring(4).trim());
            } else if (arg.startsWith("--r=")) {
                r = Integer.parseInt(arg.substring(4).trim());
            } else if (arg.startsWith("--peers=")) {
                String peersList = arg.substring(8).trim();
                if (!peersList.isEmpty()) {
                    for (String peer : peersList.split(",")) {
                        if (!peer.isBlank()) {
                            peers.add(parsePeer(peer.trim()));
                        }
                    }
                }
            }
        }

        return new ClusterConfig(id, host, port, peers, n, w, r);
    }

    /**
     * Parseaza un peer in formatul {@code id@host:port} sau {@code host:port}.
     *
     * <p>ID-ul trebuie sa fie ACELASI pe toate nodurile (inelul de hashing este construit din ID-uri;
     * ID-uri diferite ar produce inele diferite pe noduri diferite). Fara {@code id@}, ID-ul
     * implicit este {@code host:port}, deci nodul curent trebuie pornit cu {@code --id=host:port}.
     */
    static Node parsePeer(String spec) {
        String id = null;
        String address = spec;
        int at = spec.indexOf('@');
        if (at >= 0) {
            id = spec.substring(0, at);
            address = spec.substring(at + 1);
        }
        int colon = address.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException("Peer invalid (asteptat [id@]host:port): " + spec);
        }
        String pHost = address.substring(0, colon);
        int pPort = Integer.parseInt(address.substring(colon + 1));
        return new Node(id != null ? id : address, pHost, pPort);
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public int getReadQuorum() {
        return readQuorum;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public List<Node> getPeers() {
        return Collections.unmodifiableList(peers);
    }

    public Node toCurrentNode() {
        return new Node(nodeId, host, port);
    }
}
