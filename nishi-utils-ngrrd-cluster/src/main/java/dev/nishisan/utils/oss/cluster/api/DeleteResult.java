package dev.nishisan.utils.oss.cluster.api;

/**
 * An ERROR never means absence; its code and message describe the failed operation.
 * @param status per-key outcome
 * @param ownerNodeId owner, if known
 * @param lastWriteReceivedAtEpochMs conservative durable receipt upper bound on recent-write refusal
 * @param errorCode typed failure for ERROR, otherwise null
 * @param message failure detail, otherwise null
 */
public record DeleteResult(DeleteStatus status, String ownerNodeId, Long lastWriteReceivedAtEpochMs,
                           ErrorCode errorCode, String message) {
    public static DeleteResult of(DeleteStatus status, String owner) {
        return new DeleteResult(status, owner, null, null, null);
    }
    public static DeleteResult error(ErrorCode code, String message) {
        return new DeleteResult(DeleteStatus.ERROR, null, null, code, message);
    }
}
