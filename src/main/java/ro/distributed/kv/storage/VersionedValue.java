package ro.distributed.kv.storage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Valoare versionata stocata pentru o cheie.
 * Include timestamp si numar de versiune pentru Last-Write-Wins (LWW) si rezolutie de conflicte.
 */
public final class VersionedValue {

    private final byte[] data;
    private final long timestamp;
    private final long version;
    private final boolean tombstone;

    public VersionedValue(byte[] data, long timestamp, long version) {
        this(data, timestamp, version, false);
    }

    public VersionedValue(byte[] data, long timestamp, long version, boolean tombstone) {
        this.data = data != null ? data.clone() : new byte[0];
        this.timestamp = timestamp;
        this.version = version;
        this.tombstone = tombstone;
    }

    /**
     * Marca de stergere (tombstone): inlocuieste valoarea in loc sa o elimine fizic,
     * astfel incat o stergere sa se propage prin LWW si sa nu fie "inviata" de replici vechi.
     */
    public static VersionedValue tombstone(long timestamp, long version) {
        return new VersionedValue(new byte[0], timestamp, version, true);
    }

    public static VersionedValue of(String value, long timestamp, long version) {
        byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
        return new VersionedValue(bytes, timestamp, version);
    }

    public static VersionedValue of(String value) {
        return of(value, System.currentTimeMillis(), 1L);
    }

    public byte[] getData() {
        return data.clone();
    }

    public String getDataAsString() {
        return new String(data, StandardCharsets.UTF_8);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getVersion() {
        return version;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    /**
     * Ordinea Last-Write-Wins: timestamp mai mare castiga; la timestamp egal, versiunea mai mare;
     * la egalitate totala, tombstone-ul castiga (stergerea are prioritate).
     *
     * @return true daca aceasta valoare este strict mai noua decat {@code other}
     */
    public boolean isNewerThan(VersionedValue other) {
        return compareLww(other) > 0;
    }

    public int compareLww(VersionedValue other) {
        int c = Long.compare(timestamp, other.timestamp);
        if (c != 0) return c;
        c = Long.compare(version, other.version);
        if (c != 0) return c;
        return Boolean.compare(tombstone, other.tombstone);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VersionedValue that)) return false;
        return timestamp == that.timestamp &&
                version == that.version &&
                tombstone == that.tombstone &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(timestamp, version, tombstone);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "VersionedValue{" +
                "value='" + getDataAsString() + '\'' +
                ", timestamp=" + timestamp +
                ", version=" + version +
                (tombstone ? ", tombstone" : "") +
                '}';
    }
}
