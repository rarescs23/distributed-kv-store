package ro.distributed.kv.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MemoryStorageEngineTest {

    private MemoryStorageEngine storage;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
    }

    @Test
    @DisplayName("Operatii de baza: put, get, delete, containsKey")
    void shouldPerformBasicCrudOperations() {
        assertThat(storage.size()).isEqualTo(0);
        assertThat(storage.containsKey("user:1")).isFalse();

        VersionedValue val1 = VersionedValue.of("Ion Popescu", 1000L, 1L);
        storage.put("user:1", val1);

        assertThat(storage.size()).isEqualTo(1);
        assertThat(storage.containsKey("user:1")).isTrue();

        Optional<VersionedValue> retrieved = storage.get("user:1");
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getDataAsString()).isEqualTo("Ion Popescu");
        assertThat(retrieved.get().getTimestamp()).isEqualTo(1000L);

        boolean deleted = storage.delete("user:1");
        assertThat(deleted).isTrue();
        assertThat(storage.get("user:1")).isEmpty();
        assertThat(storage.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("Last-Write-Wins: putIfNewer accepta doar versiuni cu timestamp mai mare sau egal")
    void shouldEnforceLastWriteWins() {
        VersionedValue initial = VersionedValue.of("Versiunea 1", 1000L, 1L);
        storage.put("config:theme", initial);

        // Incercare de scriere mai veche (timestamp 800 < 1000)
        VersionedValue stale = VersionedValue.of("Versiunea Veche", 800L, 0L);
        boolean staleAccepted = storage.putIfNewer("config:theme", stale);
        assertThat(staleAccepted).isFalse();
        assertThat(storage.get("config:theme").get().getDataAsString()).isEqualTo("Versiunea 1");

        // Incercare de scriere mai noua (timestamp 1200 > 1000)
        VersionedValue newer = VersionedValue.of("Versiunea 2", 1200L, 2L);
        boolean newerAccepted = storage.putIfNewer("config:theme", newer);
        assertThat(newerAccepted).isTrue();
        assertThat(storage.get("config:theme").get().getDataAsString()).isEqualTo("Versiunea 2");
    }

    @Test
    @DisplayName("Concurenta: scrieri si citiri simultane din multiple thread-uri fara race condition")
    void shouldHandleConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCounter = new AtomicInteger();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key-" + (i % 50);
                        storage.put(key, VersionedValue.of("thread-" + threadId + "-val-" + i));
                        if (storage.get(key).isPresent()) {
                            successCounter.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(successCounter.get()).isEqualTo(threadCount * operationsPerThread);
        assertThat(storage.size()).isEqualTo(50);
    }
}
