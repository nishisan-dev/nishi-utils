package dev.nishisan.utils.oss.cluster.protocol;
import java.util.List;
/** A non-atomic page of independent deletions. */
public record DeleteBatchRequest(List<DeleteRequest> requests) {
    public DeleteBatchRequest {
        requests = List.copyOf(requests);
        if (requests.size() > CatalogLookupRequest.MAX_KEYS) throw new IllegalArgumentException("too many keys");
    }
}
