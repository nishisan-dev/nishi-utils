package dev.nishisan.utils.oss.cluster.protocol;
import java.util.Map;
/** Inventory/actions by series, including failures without hiding partial success. */
public record ReconcileResponse(Map<String, String> results, long quarantinedBytes) {
    public ReconcileResponse { results = Map.copyOf(results); }
}
