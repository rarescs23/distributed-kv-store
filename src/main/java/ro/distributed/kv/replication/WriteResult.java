package ro.distributed.kv.replication;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Rezultatul unui Quorum Write (PUT).
 *
 * <p>Posibile stari:
 * <ul>
 *   <li>{@link Status#SUCCESS}       - cel putin W replici au confirmat</li>
 *   <li>{@link Status#QUORUM_TIMEOUT}- nu s-au adunat W confirmari in intervalul de timeout</li>
 *   <li>{@link Status#FAILURE}       - eroare interna (inel gol, intrerupere etc.)</li>
 * </ul>
 */
public final class WriteResult {

    public enum Status {
        SUCCESS,
        QUORUM_TIMEOUT,
        FAILURE
    }

    private final Status status;
    private final int acksReceived;
    private final int totalReplicas;
    private final int requiredW;
    private final List<String> confirmedNodes;
    private final String errorMessage;

    private WriteResult(Status status,
                        int acksReceived,
                        int totalReplicas,
                        int requiredW,
                        List<String> confirmedNodes,
                        String errorMessage) {
        this.status = status;
        this.acksReceived = acksReceived;
        this.totalReplicas = totalReplicas;
        this.requiredW = requiredW;
        this.confirmedNodes = confirmedNodes != null
                ? Collections.unmodifiableList(confirmedNodes)
                : Collections.emptyList();
        this.errorMessage = errorMessage;
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    public static WriteResult success(int acksReceived, int totalReplicas, List<String> confirmedNodes) {
        return new WriteResult(Status.SUCCESS, acksReceived, totalReplicas, 0, confirmedNodes, null);
    }

    public static WriteResult quorumTimeout(int acksReceived, int totalReplicas,
                                            List<String> confirmedNodes, int requiredW) {
        return new WriteResult(Status.QUORUM_TIMEOUT, acksReceived, totalReplicas, requiredW, confirmedNodes, null);
    }

    public static WriteResult failure(String errorMessage) {
        return new WriteResult(Status.FAILURE, 0, 0, 0, Collections.emptyList(), errorMessage);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }

    public Status getStatus() {
        return status;
    }

    public int getAcksReceived() {
        return acksReceived;
    }

    public int getTotalReplicas() {
        return totalReplicas;
    }

    public int getRequiredW() {
        return requiredW;
    }

    public List<String> getConfirmedNodes() {
        return confirmedNodes;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "WriteResult{status=" + status
                + ", acks=" + acksReceived + "/" + totalReplicas
                + ", confirmedNodes=" + confirmedNodes
                + (errorMessage != null ? ", error=" + errorMessage : "")
                + "}";
    }
}
