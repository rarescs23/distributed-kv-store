package ro.distributed.kv.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Inel de Consistent Hashing cu suport pentru Noduri Virtuale (V-Nodes),
 * replicare cu deduplicare pe noduri fizice si tratare a circularitatii (wrap-around).
 *
 * Thread-safe: foloseste un ReentrantReadWriteLock pentru citiri paralele fara blocaje
 * si sincronizare sigura la adaugarea sau eliminarea nodurilor.
 */
public class ConsistentHashRing {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashRing.class);
    public static final int DEFAULT_VNODES_PER_NODE = 150;

    private final HashFunction hashFunction;
    private final int vnodesPerNode;

    private final NavigableMap<Long, VirtualNode> ring = new TreeMap<>();
    private final Set<Node> physicalNodes = new HashSet<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public ConsistentHashRing(HashFunction hashFunction) {
        this(hashFunction, DEFAULT_VNODES_PER_NODE);
    }

    public ConsistentHashRing(HashFunction hashFunction, int vnodesPerNode) {
        this.hashFunction = Objects.requireNonNull(hashFunction, "Functia de hash nu poate fi null");
        if (vnodesPerNode <= 0) {
            throw new IllegalArgumentException("Numarul de V-Nodes per nod trebuie sa fie pozitiv: " + vnodesPerNode);
        }
        this.vnodesPerNode = vnodesPerNode;
    }

    /**
     * Adauga un nod fizic pe inel, generand numarul configurat de V-Nodes.
     *
     * @param node nodul fizic de adaugat
     * @return true daca nodul a fost adaugat, false daca era deja prezent
     */
    public boolean addNode(Node node) {
        Objects.requireNonNull(node, "Nodul nu poate fi null");
        writeLock.lock();
        try {
            if (physicalNodes.contains(node)) {
                log.warn("Nodul {} este deja prezent pe inel", node.getId());
                return false;
            }

            physicalNodes.add(node);
            for (int i = 0; i < vnodesPerNode; i++) {
                VirtualNode vNode = new VirtualNode(node, i);
                long hash = hashFunction.hash(vNode.getVirtualKey());
                ring.put(hash, vNode);
            }

            log.info("Adaugat nodul {} cu {} vnodes. Total vnodes pe inel: {}",
                    node.getId(), vnodesPerNode, ring.size());
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Elimina un nod fizic si toate vnodes-urile sale de pe inel.
     *
     * @param node nodul fizic de sters
     * @return true daca nodul a fost gasit si eliminat, false altfel
     */
    public boolean removeNode(Node node) {
        Objects.requireNonNull(node, "Nodul nu poate fi null");
        writeLock.lock();
        try {
            if (!physicalNodes.remove(node)) {
                return false;
            }

            for (int i = 0; i < vnodesPerNode; i++) {
                String virtualKey = node.getId() + "#vnode-" + i;
                long hash = hashFunction.hash(virtualKey);
                ring.remove(hash);
            }

            log.info("Eliminat nodul {} de pe inel. Vnodes ramase: {}", node.getId(), ring.size());
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Gaseste nodul fizic primar responsabil pentru o cheie.
     *
     * @param key cheia pentru care cautam nodul
     * @return nodul fizic sau null daca inelul este gol
     */
    public Node getNode(String key) {
        List<Node> preferenceList = getPreferenceList(key, 1);
        return preferenceList.isEmpty() ? null : preferenceList.get(0);
    }

    /**
     * Calculeaza lista de preferinta (cei N succesori fizici unici) pentru replicare.
     *
     * Parcurge inelul in sensul acelor de ceasornic incepand de la hash(key).
     * Daca atinge capatul superior al inelului, face wrap-around de la inceput.
     * Elimina automat duplicatele (V-nodes ale aceluiasi nod fizic).
     *
     * @param key cheia cautata
     * @param n numarul de replici dorite
     * @return lista ordonata de noduri fizice unice responsabile (maxim physicalNodes.size())
     */
    public List<Node> getPreferenceList(String key, int n) {
        Objects.requireNonNull(key, "Cheia nu poate fi null");
        if (n <= 0) {
            return Collections.emptyList();
        }

        readLock.lock();
        try {
            if (ring.isEmpty()) {
                return Collections.emptyList();
            }

            int targetCount = Math.min(n, physicalNodes.size());
            Set<Node> distinctNodes = new LinkedHashSet<>(targetCount);

            long keyHash = hashFunction.hash(key);

            // 1. Cautam de la keyHash inainte (in sensul acelor de ceasornic)
            NavigableMap<Long, VirtualNode> tailMap = ring.tailMap(keyHash, true);
            for (VirtualNode vnode : tailMap.values()) {
                distinctNodes.add(vnode.getPhysicalNode());
                if (distinctNodes.size() == targetCount) {
                    return new ArrayList<>(distinctNodes);
                }
            }

            // 2. Wrap-around circular: daca nu am strans N noduri unice, continuam de la inceputul inelului
            for (VirtualNode vnode : ring.values()) {
                distinctNodes.add(vnode.getPhysicalNode());
                if (distinctNodes.size() == targetCount) {
                    break;
                }
            }

            return new ArrayList<>(distinctNodes);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returneaza multimea nodurilor fizice din inel.
     */
    public Set<Node> getPhysicalNodes() {
        readLock.lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(physicalNodes));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Numarul de noduri fizice active.
     */
    public int getPhysicalNodeCount() {
        readLock.lock();
        try {
            return physicalNodes.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Numarul total de noduri virtuale de pe inel.
     */
    public int getVNodesCount() {
        readLock.lock();
        try {
            return ring.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returneaza o copie a inelului (hash -> VirtualNode) pentru inspectie si vizualizare.
     */
    public NavigableMap<Long, VirtualNode> getRingSnapshot() {
        readLock.lock();
        try {
            return new TreeMap<>(ring);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Verifica daca inelul contine cel putin un nod.
     */
    public boolean isEmpty() {
        readLock.lock();
        try {
            return ring.isEmpty();
        } finally {
            readLock.unlock();
        }
    }
}
