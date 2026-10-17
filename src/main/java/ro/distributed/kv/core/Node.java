package ro.distributed.kv.core;

import java.util.Objects;

/**
 * Reprezinta un nod fizic (server) din clusterul distribuit.
 */
public class Node {

    private final String id;
    private final String host;
    private final int port;

    public Node(String id, String host, int port) {
        this.id = Objects.requireNonNull(id, "ID-ul nodului nu poate fi null");
        this.host = Objects.requireNonNull(host, "Host-ul nu poate fi null");
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port invalid: " + port);
        }
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getAddress() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node other)) return false;
        return Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
