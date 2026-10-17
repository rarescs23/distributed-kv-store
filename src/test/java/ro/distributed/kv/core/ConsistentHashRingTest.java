package ro.distributed.kv.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class ConsistentHashRingTest {

    private HashFunction hashFunction;
    private ConsistentHashRing ring;

    private Node node1;
    private Node node2;
    private Node node3;

    @BeforeEach
    void setUp() {
        hashFunction = new Murmur3Hash();
        ring = new ConsistentHashRing(hashFunction, 150);

        node1 = new Node("node-1", "127.0.0.1", 50051);
        node2 = new Node("node-2", "127.0.0.1", 50052);
        node3 = new Node("node-3", "127.0.0.1", 50053);
    }

    @Test
    @DisplayName("Adaugarea si stergerea de noduri actualizeaza corect dimensiunea inelului")
    void shouldAddAndRemoveNodesCorrectly() {
        assertThat(ring.isEmpty()).isTrue();
        assertThat(ring.getPhysicalNodeCount()).isEqualTo(0);

        ring.addNode(node1);
        ring.addNode(node2);

        assertThat(ring.getPhysicalNodeCount()).isEqualTo(2);
        assertThat(ring.getVNodesCount()).isEqualTo(300); // 2 * 150
        assertThat(ring.getPhysicalNodes()).containsExactlyInAnyOrder(node1, node2);

        ring.removeNode(node1);
        assertThat(ring.getPhysicalNodeCount()).isEqualTo(1);
        assertThat(ring.getVNodesCount()).isEqualTo(150);
        assertThat(ring.getPhysicalNodes()).containsExactly(node2);
    }

    @Test
    @DisplayName("Inelul returneaza null pentru rutare daca nu exista niciun nod")
    void shouldReturnNullWhenRingIsEmpty() {
        assertThat(ring.getNode("any-key")).isNull();
        assertThat(ring.getPreferenceList("any-key", 3)).isEmpty();
    }

    @Test
    @DisplayName("getPreferenceList trebuie sa returneze noduri fizice unice, fara duplicate de V-nodes")
    void shouldReturnDistinctPhysicalNodesInPreferenceList() {
        ring.addNode(node1);
        ring.addNode(node2);
        ring.addNode(node3);

        List<Node> replicas = ring.getPreferenceList("user:profile:42", 3);

        assertThat(replicas).hasSize(3);
        // Trebuie sa fie unice si sa cuprinda toate cele 3 noduri
        assertThat(new HashSet<>(replicas)).hasSize(3);
        assertThat(replicas).containsExactlyInAnyOrder(node1, node2, node3);
    }

    @Test
    @DisplayName("Daca se cer mai multe replici decat noduri existente, se returneaza maximul posibil")
    void shouldNotExceedAvailablePhysicalNodes() {
        ring.addNode(node1);
        ring.addNode(node2);

        List<Node> replicas = ring.getPreferenceList("order:999", 5);

        assertThat(replicas).hasSize(2);
        assertThat(replicas).containsExactlyInAnyOrder(node1, node2);
    }

    @Test
    @DisplayName("Wrap-around: rutarea functioneaza si cand hash-ul cheii depaseste cel mai mare hash de nod")
    void shouldWrapAroundSuccessfully() {
        // Folosim o functie de hash mockuita pentru a controla pozitiile exacte
        Map<String, Long> fixedHashes = new HashMap<>();
        fixedHashes.put("node-A#vnode-0", 100L);
        fixedHashes.put("node-B#vnode-0", 500L);
        fixedHashes.put("key-after-last", 900L);

        ConsistentHashRing controlledRing = new ConsistentHashRing(key -> fixedHashes.getOrDefault(key, 0L), 1);
        Node nodeA = new Node("node-A", "localhost", 1001);
        Node nodeB = new Node("node-B", "localhost", 1002);

        controlledRing.addNode(nodeA);
        controlledRing.addNode(nodeB);

        // key-after-last are hash 900, mai mare decat nodurile 100 si 500
        // Trebuie sa faca wrap-around la primul nod (nodeA la pozitia 100)
        Node primary = controlledRing.getNode("key-after-last");
        assertThat(primary).isEqualTo(nodeA);

        List<Node> replicas = controlledRing.getPreferenceList("key-after-last", 2);
        assertThat(replicas).containsExactly(nodeA, nodeB);
    }

    @Test
    @DisplayName("La stergerea unui nod, doar cheile sale sunt remapate la urmatorul vecin")
    void shouldRemapMinimallyOnNodeRemoval() {
        ring.addNode(node1);
        ring.addNode(node2);
        ring.addNode(node3);

        int totalKeys = 1000;
        Map<String, Node> initialAssignments = new HashMap<>();
        for (int i = 0; i < totalKeys; i++) {
            String key = "session:" + i;
            initialAssignments.put(key, ring.getNode(key));
        }

        // Eliminam node2
        ring.removeNode(node2);

        int unchanged = 0;
        int remappedFromNode2 = 0;

        for (int i = 0; i < totalKeys; i++) {
            String key = "session:" + i;
            Node oldNode = initialAssignments.get(key);
            Node newNode = ring.getNode(key);

            if (oldNode.equals(node2)) {
                // Cheile de pe node2 trebuie sa fi fost redistribuite catre node1 sau node3
                assertThat(newNode).isIn(node1, node3);
                remappedFromNode2++;
            } else {
                // Cheile care erau pe node1 sau node3 ar trebui sa ramana preponderent pe acelasi nod
                if (oldNode.equals(newNode)) {
                    unchanged++;
                }
            }
        }

        // Cel putin 90% din cheile celorlalte noduri raman neschimbate
        int totalOnOtherNodes = totalKeys - remappedFromNode2;
        double retentionRate = (double) unchanged / totalOnOtherNodes;
        assertThat(retentionRate).isGreaterThan(0.95);
    }

    @Test
    @DisplayName("Distributie uniforma: 100.000 de chei pe 5 noduri au o deviatie standard sub 15%")
    void shouldDistributeKeysUniformlyAcrossNodes() {
        ConsistentHashRing largeRing = new ConsistentHashRing(hashFunction, 150);

        int numNodes = 5;
        List<Node> nodes = new ArrayList<>();
        for (int i = 1; i <= numNodes; i++) {
            Node n = new Node("server-" + i, "10.0.0." + i, 8080);
            nodes.add(n);
            largeRing.addNode(n);
        }

        int totalKeys = 100_000;
        Map<Node, Integer> distribution = new HashMap<>();
        for (Node n : nodes) {
            distribution.put(n, 0);
        }

        for (int i = 0; i < totalKeys; i++) {
            String key = "benchmark-key-" + UUID.randomUUID();
            Node assigned = largeRing.getNode(key);
            distribution.put(assigned, distribution.get(assigned) + 1);
        }

        double expectedPerNode = (double) totalKeys / numNodes; // 20.000

        // Calcul deviatie standard
        double varianceSum = 0;
        for (int count : distribution.values()) {
            varianceSum += Math.pow(count - expectedPerNode, 2);
        }
        double stdDev = Math.sqrt(varianceSum / numNodes);
        double coefficientOfVariationPercent = (stdDev / expectedPerNode) * 100;

        System.out.printf("Distributie pe 100.000 chei (5 noduri x 150 vnodes):%n");
        distribution.forEach((n, count) ->
                System.out.printf("  %s: %d chei (%.2f%%)%n", n.getId(), count, (count * 100.0 / totalKeys)));
        System.out.printf("  Media: %.0f | Deviatie Standard: %.2f | Coeficient Variatie: %.2f%%%n",
                expectedPerNode, stdDev, coefficientOfVariationPercent);

        // Coeficientul de variatie trebuie sa fie semnificativ sub 15% (de regula intre 3% si 8%)
        assertThat(coefficientOfVariationPercent).isLessThan(15.0);
    }
}
