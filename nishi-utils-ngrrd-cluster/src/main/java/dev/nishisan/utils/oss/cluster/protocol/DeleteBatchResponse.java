package dev.nishisan.utils.oss.cluster.protocol;
import dev.nishisan.utils.oss.cluster.api.DeleteResult;
import java.util.Map;
/** Contains one outcome for every requested key. */
public record DeleteBatchResponse(Map<String, DeleteResult> results) {
    public DeleteBatchResponse { results = Map.copyOf(results); }
}
