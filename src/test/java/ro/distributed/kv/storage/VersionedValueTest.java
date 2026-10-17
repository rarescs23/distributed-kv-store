package ro.distributed.kv.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VersionedValueTest {

    @Test
    @DisplayName("LWW: timestamp-ul mai mare castiga, apoi versiunea, apoi tombstone-ul")
    void shouldOrderByTimestampThenVersionThenTombstone() {
        VersionedValue old = VersionedValue.of("a", 100L, 5L);
        VersionedValue newerTs = VersionedValue.of("b", 200L, 1L);
        VersionedValue sameTsHigherVersion = VersionedValue.of("c", 100L, 6L);
        VersionedValue sameAllTombstone = VersionedValue.tombstone(100L, 5L);

        assertThat(newerTs.isNewerThan(old)).isTrue();
        assertThat(old.isNewerThan(newerTs)).isFalse();
        assertThat(sameTsHigherVersion.isNewerThan(old)).isTrue();
        assertThat(sameAllTombstone.isNewerThan(old)).isTrue();
        assertThat(old.isNewerThan(sameAllTombstone)).isFalse();
        assertThat(old.isNewerThan(old)).isFalse();
    }

    @Test
    @DisplayName("Tombstone-ul este marcat si nu este egal cu o valoare goala obisnuita")
    void tombstoneShouldDifferFromEmptyValue() {
        VersionedValue tomb = VersionedValue.tombstone(10L, 1L);
        VersionedValue empty = new VersionedValue(new byte[0], 10L, 1L);

        assertThat(tomb.isTombstone()).isTrue();
        assertThat(empty.isTombstone()).isFalse();
        assertThat(tomb).isNotEqualTo(empty);
    }

    @Test
    @DisplayName("putIfNewer: un tombstone mai nou inlocuieste valoarea, o valoare veche nu inlocuieste tombstone-ul")
    void putIfNewerShouldRespectTombstones() {
        MemoryStorageEngine storage = new MemoryStorageEngine();
        storage.put("k", VersionedValue.of("v", 100L, 1L));

        assertThat(storage.putIfNewer("k", VersionedValue.tombstone(200L, 2L))).isTrue();
        assertThat(storage.get("k").get().isTombstone()).isTrue();

        assertThat(storage.putIfNewer("k", VersionedValue.of("zombie", 150L, 1L))).isFalse();
        assertThat(storage.get("k").get().isTombstone()).isTrue();
    }
}
