package ro.distributed.kv.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ro.distributed.kv.core.Node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterConfigTest {

    @Test
    @DisplayName("Peers cu id explicit pastreaza ID-ul, ca inelul sa fie identic pe toate nodurile")
    void shouldParsePeersWithExplicitIds() {
        ClusterConfig cfg = ClusterConfig.fromArgs(new String[]{
                "--id=node-1", "--port=50051",
                "--peers=node-2@localhost:50052,node-3@10.0.0.3:50053"});

        assertThat(cfg.getPeers()).extracting(Node::getId).containsExactly("node-2", "node-3");
        assertThat(cfg.getPeers().get(1).getHost()).isEqualTo("10.0.0.3");
        assertThat(cfg.getPeers().get(1).getPort()).isEqualTo(50053);
    }

    @Test
    @DisplayName("Peer fara id foloseste host:port ca ID determinist")
    void shouldDeriveDeterministicIdFromAddress() {
        ClusterConfig cfg = ClusterConfig.fromArgs(new String[]{"--peers=localhost:50052"});
        assertThat(cfg.getPeers().get(0).getId()).isEqualTo("localhost:50052");
    }

    @Test
    @DisplayName("Parametrii N/W/R au valori implicite 3/2/2 si pot fi suprascrisi")
    void shouldParseQuorumSettings() {
        ClusterConfig defaults = ClusterConfig.fromArgs(new String[]{});
        assertThat(defaults.getReplicationFactor()).isEqualTo(3);
        assertThat(defaults.getWriteQuorum()).isEqualTo(2);
        assertThat(defaults.getReadQuorum()).isEqualTo(2);

        ClusterConfig custom = ClusterConfig.fromArgs(new String[]{"--n=5", "--w=3", "--r=3"});
        assertThat(custom.getReplicationFactor()).isEqualTo(5);
        assertThat(custom.getWriteQuorum()).isEqualTo(3);
        assertThat(custom.getReadQuorum()).isEqualTo(3);
    }

    @Test
    @DisplayName("Peer invalid este respins")
    void shouldRejectMalformedPeer() {
        assertThatThrownBy(() -> ClusterConfig.fromArgs(new String[]{"--peers=fara-port"}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
