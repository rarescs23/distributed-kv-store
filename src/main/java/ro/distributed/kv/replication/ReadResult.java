package ro.distributed.kv.replication;

import ro.distributed.kv.storage.VersionedValue;

import java.util.Optional;

/**
 * Rezultatul unui Quorum Read (GET).
 *
 * <p>Posibile stari:
 * <ul>
 *   <li>{@link Status#FOUND}         - cheia a fost gasita, valoarea LWW este disponibila</li>
 *   <li>{@link Status#NOT_FOUND}     - cel putin R replici au raspuns, dar cheia nu exista pe niciuna</li>
 *   <li>{@link Status#QUORUM_TIMEOUT}- nu s-au adunat R raspunsuri in intervalul de timeout</li>
 *   <li>{@link Status#FAILURE}       - eroare interna (inel gol, intrerupere etc.)</li>
 * </ul>
 */
public final class ReadResult {

    public enum Status {
        FOUND,
        NOT_FOUND,
        QUORUM_TIMEOUT,
        FAILURE
    }

    private final Status status;
    private final VersionedValue value;
    private final int responsesReceived;
    private final int requiredR;
    private final String errorMessage;

    private ReadResult(Status status,
                       VersionedValue value,
                       int responsesReceived,
                       int requiredR,
                       String errorMessage) {
        this.status = status;
        this.value = value;
        this.responsesReceived = responsesReceived;
        this.requiredR = requiredR;
        this.errorMessage = errorMessage;
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    public static ReadResult found(VersionedValue value, int responsesReceived) {
        return new ReadResult(Status.FOUND, value, responsesReceived, 0, null);
    }

    public static ReadResult notFound(int responsesReceived) {
        return new ReadResult(Status.NOT_FOUND, null, responsesReceived, 0, null);
    }

    public static ReadResult quorumTimeout(int responsesReceived, int requiredR) {
        return new ReadResult(Status.QUORUM_TIMEOUT, null, responsesReceived, requiredR, null);
    }

    public static ReadResult failure(String errorMessage) {
        return new ReadResult(Status.FAILURE, null, 0, 0, errorMessage);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public boolean isFound() {
        return status == Status.FOUND;
    }

    public boolean isSuccess() {
        return status == Status.FOUND || status == Status.NOT_FOUND;
    }

    public Status getStatus() {
        return status;
    }

    /**
     * Valoarea LWW, prezenta doar daca status == FOUND.
     */
    public Optional<VersionedValue> getValue() {
        return Optional.ofNullable(value);
    }

    public int getResponsesReceived() {
        return responsesReceived;
    }

    public int getRequiredR() {
        return requiredR;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "ReadResult{status=" + status
                + ", responses=" + responsesReceived
                + (value != null ? ", ts=" + value.getTimestamp() : "")
                + (errorMessage != null ? ", error=" + errorMessage : "")
                + "}";
    }
}
