package ro.distributed.kv.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementare in memorie a motorului de stocare, thread-safe, bazata pe ConcurrentHashMap.
 */
public class MemoryStorageEngine implements StorageEngine {

    private static final Logger log = LoggerFactory.getLogger(MemoryStorageEngine.class);

    private final ConcurrentMap<String, VersionedValue> store = new ConcurrentHashMap<>();

    @Override
    public void put(String key, VersionedValue value) {
        Objects.requireNonNull(key, "Cheia nu poate fi null");
        Objects.requireNonNull(value, "Valoarea nu poate fi null");
        store.put(key, value);
    }

    /**
     * Salveaza valoarea doar daca nu este mai veche decat versiunea existenta (Last-Write-Wins).
     *
     * @return true daca a fost aplicata, false daca valoarea existenta era mai noua
     */
    @Override
    public boolean putIfNewer(String key, VersionedValue newValue) {
        Objects.requireNonNull(key, "Cheia nu poate fi null");
        Objects.requireNonNull(newValue, "Valoarea nu poate fi null");

        boolean[] applied = {false};
        store.compute(key, (k, existing) -> {
            if (existing == null || newValue.compareLww(existing) >= 0) {
                applied[0] = true;
                return newValue;
            }
            log.debug("Ignorata scrierea invechita pentru cheia {}: nou timestamp={}, existent={}",
                    key, newValue.getTimestamp(), existing.getTimestamp());
            return existing;
        });
        return applied[0];
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        Objects.requireNonNull(key, "Cheia nu poate fi null");
        return Optional.ofNullable(store.get(key));
    }

    @Override
    public boolean delete(String key) {
        Objects.requireNonNull(key, "Cheia nu poate fi null");
        return store.remove(key) != null;
    }

    @Override
    public boolean containsKey(String key) {
        Objects.requireNonNull(key, "Cheia nu poate fi null");
        return store.containsKey(key);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public void clear() {
        store.clear();
    }
}
