package ro.distributed.kv.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class RingVisualizerTest {

    @Test
    @DisplayName("Vizualizeaza structura completa a inelului: noduri fizice, virtuale si rutarea cheilor")
    void shouldPrintRingStructureAndRouting() {
        // Folosim un seed fix si 4 vnodes per nod pentru o afisare compacta si clara in consola
        int vnodesPerNode = 4;
        HashFunction hashFunction = new Murmur3Hash(42);
        ConsistentHashRing ring = new ConsistentHashRing(hashFunction, vnodesPerNode);

        Node node1 = new Node("Node-A (Bucuresti)", "192.168.1.10", 50051);
        Node node2 = new Node("Node-B (Frankfurt)", "192.168.1.20", 50052);
        Node node3 = new Node("Node-C (Dublin)",    "192.168.1.30", 50053);

        ring.addNode(node1);
        ring.addNode(node2);
        ring.addNode(node3);

        // ==========================================
        // 1. AFISARE NODURI FIZICE
        // ==========================================
        System.out.println("==========================================================================================");
        System.out.println(" 🖥️  NODURILE FIZICE DIN CLUSTER (" + ring.getPhysicalNodeCount() + " noduri active)");
        System.out.println("==========================================================================================");
        System.out.printf("%-25s | %-16s | %-6s | %-10s%n", "Nod ID", "Host IP", "Port", "V-Nodes");
        System.out.println("------------------------------------------------------------------------------------------");
        for (Node node : ring.getPhysicalNodes()) {
            System.out.printf("%-25s | %-16s | %-6d | %-10d%n",
                    node.getId(), node.getHost(), node.getPort(), vnodesPerNode);
        }
        System.out.println();

        // ==========================================
        // 2. AFISARE INEL SORTAT DUPA HASH (V-NODES)
        // ==========================================
        NavigableMap<Long, VirtualNode> snapshot = ring.getRingSnapshot();
        System.out.println("==========================================================================================");
        System.out.println(" 🪐 INELUL DE CONSISTENT HASHING (ORDONAT CIRCULAR DUPA HASH PE 64-BIT)");
        System.out.println(" Total puncte pe inel (V-Nodes): " + snapshot.size());
        System.out.println(" Interval numeric: [" + Long.MIN_VALUE + " ... " + Long.MAX_VALUE + "]");
        System.out.println("==========================================================================================");
        System.out.printf("%-4s | %-22s | %-25s | %-25s%n",
                "Poz", "Hash (Token pe Inel)", "Token Nod Virtual", "Nod Fizic Parinte");
        System.out.println("------------------------------------------------------------------------------------------");

        int index = 1;
        for (Map.Entry<Long, VirtualNode> entry : snapshot.entrySet()) {
            long hash = entry.getKey();
            VirtualNode vnode = entry.getValue();
            System.out.printf("#%-3d | %22d | %-25s | %-25s%n",
                    index++,
                    hash,
                    vnode.getVirtualKey(),
                    vnode.getPhysicalNode().getId());
        }
        System.out.println("------------------------------------------------------------------------------------------");
        System.out.println("🔄 NOTA: Dupa ultima pozitie (#" + snapshot.size() + "), inelul face WRAP-AROUND inapoi la pozitia #1!");
        System.out.println();

        // ==========================================
        // 3. SIMULARE RUTARE CHEI & REPLICARE
        // ==========================================
        List<String> sampleKeys = List.of(
                "user:1001",
                "user:1002",
                "order:99482",
                "cart:session_abc",
                "payment:tx_8819",
                "product:laptop-dell"
        );

        System.out.println("==========================================================================================");
        System.out.println(" 🎯 RUTARE CHEI PE INEL (MAPPING SI LISTA DE PREFERINTA PENTRU REPLICARE N=3)");
        System.out.println("==========================================================================================");
        System.out.printf("%-20s | %22s | %-25s | %-30s%n",
                "Cheie Client", "Hash Cheie", "Nod Primar (Lider)", "Replicare N=3 (Succesori Unici)");
        System.out.println("------------------------------------------------------------------------------------------");

        for (String key : sampleKeys) {
            long keyHash = hashFunction.hash(key);
            Node primaryNode = ring.getNode(key);
            List<Node> replicas = ring.getPreferenceList(key, 3);

            StringBuilder replicasStr = new StringBuilder();
            for (int i = 0; i < replicas.size(); i++) {
                replicasStr.append(replicas.get(i).getId());
                if (i < replicas.size() - 1) {
                    replicasStr.append(" -> ");
                }
            }

            System.out.printf("%-20s | %22d | %-25s | %-30s%n",
                    key, keyHash, primaryNode.getId(), replicasStr);
        }
        System.out.println("==========================================================================================");

        // Verificari formale JUnit
        assertThat(ring.getPhysicalNodeCount()).isEqualTo(3);
        assertThat(snapshot).hasSize(12); // 3 noduri * 4 vnodes
    }

    @Test
    @DisplayName("Demonstreaza explicit mecanismul de Wrap-Around pe inel")
    void shouldDemonstrateWrapAroundVisually() {
        HashFunction hashFunction = new Murmur3Hash();
        ConsistentHashRing ring = new ConsistentHashRing(hashFunction, 2);

        Node node1 = new Node("Node-1", "127.0.0.1", 5001);
        Node node2 = new Node("Node-2", "127.0.0.1", 5002);

        ring.addNode(node1);
        ring.addNode(node2);

        NavigableMap<Long, VirtualNode> snapshot = ring.getRingSnapshot();
        long maxRingHash = snapshot.lastKey();
        long minRingHash = snapshot.firstKey();
        VirtualNode firstVNode = snapshot.firstEntry().getValue();
        VirtualNode lastVNode = snapshot.lastEntry().getValue();

        System.out.println();
        System.out.println("==========================================================================================");
        System.out.println(" 🔄 DEMONSTRATIE VIZUALA: WRAP-AROUND LA CAPATUL INELULUI");
        System.out.println("==========================================================================================");
        System.out.println("Primul nod pe inel (hash minim): " + minRingHash + " -> " + firstVNode.getVirtualKey());
        System.out.println("Ultimul nod pe inel (hash maxim): " + maxRingHash + " -> " + lastVNode.getVirtualKey());
        System.out.println();
        System.out.println("Daca o cheie are hash-ul mai mare decat " + maxRingHash + ":");
        System.out.println("Ea nu gaseste niciun succesor direct dupa ea pe axa numerica.");
        System.out.println("Inelul sare circular inapoi la inceput si alege nodul cu hash minim: " + firstVNode.getPhysicalNode().getId());
        System.out.println("==========================================================================================");
        System.out.println();
    }
}
