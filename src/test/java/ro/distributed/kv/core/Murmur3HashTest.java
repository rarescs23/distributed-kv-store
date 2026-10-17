package ro.distributed.kv.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Murmur3HashTest {

    private final HashFunction hashFunction = new Murmur3Hash();

    @Test
    @DisplayName("Hash-ul trebuie sa fie determinist pentru aceeasi cheie")
    void shouldProduceDeterministicHash() {
        long hash1 = hashFunction.hash("user:1001");
        long hash2 = hashFunction.hash("user:1001");

        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    @DisplayName("Chei usor diferite trebuie sa produca hash-uri distincte (avalanche effect)")
    void shouldProduceDifferentHashesForSimilarKeys() {
        long hash1 = hashFunction.hash("user:1001");
        long hash2 = hashFunction.hash("user:1002");

        assertThat(hash1).isNotEqualTo(hash2);
    }

    @Test
    @DisplayName("Nu trebuie sa accepte chei null")
    void shouldThrowExceptionForNullKey() {
        assertThatThrownBy(() -> hashFunction.hash(null))
                .isInstanceOf(NullPointerException.class);
    }
}
