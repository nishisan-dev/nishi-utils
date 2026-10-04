package dev.nishisan.utils.oss.cluster.protocol;
/** REPORT is read-only; ADOPT resolves quarantine; PURGE revalidates absence or a confirmed live copy at another owner. */
public record ReconcileRequest(Action action, String seriesKey) {
    public enum Action { REPORT, ADOPT, PURGE }
    public ReconcileRequest { if (action == null) action = Action.REPORT; }
}
