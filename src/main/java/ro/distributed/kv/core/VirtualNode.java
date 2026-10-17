package ro.distributed.kv.core;

import java.util.Objects;

/**
 * Reprezinta un nod virtual (V-Node) plasat pe inelul de hashing.
 * Contribuie la dispersia uniforma a cheilor si impiedica aparitia zonelor fierbinti (hotspots).
 */
public class VirtualNode {

    private final Node physicalNode;
    private final int vnodeIndex;
    private final String virtualKey;

    public VirtualNode(Node physicalNode, int vnodeIndex) {
        this.physicalNode = Objects.requireNonNull(physicalNode, "Nodul fizic nu poate fi null");
        this.vnodeIndex = vnodeIndex;
        this.virtualKey = physicalNode.getId() + "#vnode-" + vnodeIndex;
    }

    public Node getPhysicalNode() {
        return physicalNode;
    }

    public int getVnodeIndex() {
        return vnodeIndex;
    }

    public String getVirtualKey() {
        return virtualKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VirtualNode other)) return false;
        return Objects.equals(virtualKey, other.virtualKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(virtualKey);
    }

    @Override
    public String toString() {
        return "VirtualNode{" +
                "virtualKey='" + virtualKey + '\'' +
                ", physicalNode=" + physicalNode.getId() +
                '}';
    }
}
