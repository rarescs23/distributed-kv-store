package ro.distributed.kv.core;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Implementare a functiei de hash bazata pe MurmurHash3 (128-bit trunchiat la 64-bit).
 * Ofera dispersie uniforma, rezistenta excelenta la coliziuni si viteza superioara.
 */
public class Murmur3Hash implements HashFunction {

    private final int seed;

    public Murmur3Hash() {
        this(0);
    }

    public Murmur3Hash(int seed) {
        this.seed = seed;
    }

    @Override
    public long hash(String key) {
        Objects.requireNonNull(key, "Cheia nu poate fi null");
        return Hashing.murmur3_128(seed)
                .hashString(key, StandardCharsets.UTF_8)
                .asLong();
    }
}
