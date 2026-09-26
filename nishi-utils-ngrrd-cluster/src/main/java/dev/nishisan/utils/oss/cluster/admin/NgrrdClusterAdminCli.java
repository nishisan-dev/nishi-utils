/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.admin;

import dev.nishisan.utils.oss.cluster.NgrrdCluster;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.RebalanceTrigger;
import dev.nishisan.utils.oss.cluster.catalog.CatalogReplicaStatus;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminForgetResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CLI de administração do cluster ngrrd (seção 4 da spec do M4):
 * {@code java -cp ... dev.nishisan.utils.oss.cluster.admin.NgrrdClusterAdminCli --seed host:port
 * [--client-id x] <status|metrics <nodeId>|drain <nodeId>|activate <nodeId>|forget-node <nodeId>|rebalance>}.
 *
 * <p>Entra na malha como cliente transparente ({@code roles client+leader-ineligible}, o mesmo papel de
 * {@link NgrrdClusterClient}), executa um único comando e sai — sem dependência de nenhuma biblioteca de
 * CLI, saída em texto tabular simples. {@link #run(String[], PrintStream, PrintStream)} é o método
 * testável (usado tanto por {@code main} quanto pelos testes, inclusive de cluster real).</p>
 */
public final class NgrrdClusterAdminCli {

    private static final String USAGE = "uso: NgrrdClusterAdminCli --seed host:port [--client-id x] "
            + "<status|metrics <nodeId>|drain <nodeId>|activate <nodeId>|forget-node <nodeId>|rebalance>";

    public static void main(String[] args) {
        int exitCode = new NgrrdClusterAdminCli().run(args, System.out, System.err);
        System.exit(exitCode);
    }

    /**
     * Executa um único comando administrativo e devolve o código de saída (0 = sucesso, 1 = falha).
     * Nunca lança — toda falha (parsing de argumentos, conexão, erro remoto) é reportada em {@code err}.
     */
    public int run(String[] args, PrintStream out, PrintStream err) {
        return run(args, out, err, NgrrdCluster::connect);
    }

    /**
     * Como {@link #run(String[], PrintStream, PrintStream)}, mas com a conexão ao cluster injetável —
     * visível para teste: permite substituir {@link NgrrdCluster#connect} por um {@link NgrrdClusterClient}
     * fake (sem subir um {@code NGridNode}/rede de verdade) para cobrir parsing de argumentos, formatação
     * de saída e código de retorno.
     */
    int run(String[] args, PrintStream out, PrintStream err,
            Function<NgrrdClusterConfig, NgrrdClusterClient> clientFactory) {
        ParsedArgs parsed;
        try {
            parsed = ParsedArgs.parse(args);
        } catch (IllegalArgumentException e) {
            err.println("erro: " + e.getMessage());
            err.println(USAGE);
            return 1;
        }

        NgrrdClusterConfig config = NgrrdClusterConfig.builder()
                .clientId(parsed.clientId)
                .seed(parsed.seed)
                .build();
        NgrrdClusterClient client;
        try {
            client = clientFactory.apply(config);
        } catch (RuntimeException e) {
            err.println("erro ao conectar ao cluster: " + e.getMessage());
            return 1;
        }
        try {
            return execute(parsed, client, out, err);
        } finally {
            client.close();
        }
    }

    private int execute(ParsedArgs parsed, NgrrdClusterClient client, PrintStream out, PrintStream err) {
        try {
            switch (parsed.command) {
                case "status" -> {
                    printStatus(client.clusterStatus(), out);
                    return 0;
                }
                case "metrics" -> {
                    printMetrics(client.nodeMetrics(parsed.nodeId), out);
                    return 0;
                }
                case "drain" -> {
                    printNodeStatus("drain", client.drainNode(parsed.nodeId), out);
                    return 0;
                }
                case "activate" -> {
                    printNodeStatus("activate", client.activateNode(parsed.nodeId), out);
                    return 0;
                }
                case "forget-node" -> {
                    return printForget(client.forgetNode(parsed.nodeId), out, err);
                }
                case "rebalance" -> {
                    printRebalance(client.triggerRebalance(), out);
                    return 0;
                }
                default -> {
                    err.println("erro: comando desconhecido: " + parsed.command);
                    err.println(USAGE);
                    return 1;
                }
            }
        } catch (NgrrdClusterException e) {
            err.println("erro (" + e.code() + "): " + e.getMessage());
            return 1;
        }
    }

    private void printStatus(AdminStatusResponse response, PrintStream out) {
        out.println("LIDER: " + response.leaderNodeId());
        // Issue #167 (item 3): regras do líder; cada nó mostra o próprio fingerprint, com "!" quando diverge.
        out.println("REGRAS: " + orDash(response.placementRulesHash()) + " (" + response.placementRulesCount()
                + " regras)");
        out.printf(Locale.ROOT, "%-24s %-10s %-10s %8s %14s %7s %10s %10s %14s %20s %9s %8s %s%n", "NODE", "STATE",
                "REACHABLE", "SERIES", "BYTES", "FILL%", "MODE", "WEIGHT", "RESERVED", "QUOTA", "RULES", "CAT_LAG",
                "CAPABILITIES");
        for (NodeStatusView view : response.nodes()) {
            StorageNodeStatus status = view.status();
            out.printf(Locale.ROOT, "%-24s %-10s %-10s %8d %14d %6.1f%% %10s %10.3f %14d %20s %9s %8s %s%n",
                    status.nodeId(), status.state(), view.reachable(), status.seriesCount(), status.usedBytes(),
                    status.fillRatio() * 100.0, status.distributionMode(), status.weight(), status.reservedBytes(),
                    formatQuota(status), formatRules(status, response.placementRulesHash()),
                    CatalogReplicaStatus.describeLag(status.catalogReplica()), formatCapabilities(status));
        }
        out.println("MIGRACOES EM CURSO: " + response.migrationsInFlight());
        out.println("GEOMETRIAS PENDENTES: " + response.geometriesPending());
    }

    /** {@code <maxSeries|->/<maxBytes|->}: cota dura do nó; {@code -} = sem limite. */
    private static String formatQuota(StorageNodeStatus status) {
        return (status.quotaMaxSeries() > 0 ? String.valueOf(status.quotaMaxSeries()) : "-") + "/"
                + (status.quotaMaxBytes() > 0 ? String.valueOf(status.quotaMaxBytes()) : "-");
    }

    /**
     * Primeiros 8 hex do fingerprint das regras do nó ({@code -} sem regras), com {@code !} quando difere do
     * fingerprint do líder — as regras são configuração uniforme, então {@code !} indica um nó reiniciado com
     * outro YAML (ou ainda não reiniciado após uma mudança).
     */
    private static String formatRules(StorageNodeStatus status, String leaderHash) {
        String own = status.placementRulesHash();
        String shown = own == null ? "-" : own.substring(0, Math.min(8, own.length()));
        return Objects.equals(own, leaderHash) ? shown : shown + "!";
    }

    private static String orDash(String value) {
        return value == null ? "-" : value;
    }

    /**
     * Capacidades anunciadas pelo nó, em ordem alfabética e separadas por vírgula; {@code -} quando o
     * status não traz nenhuma (storage de versão anterior, ou status regravado por um líder anterior a
     * elas) — é por aqui que o operador confirma que todos os storages já as reportam.
     */
    private static String formatCapabilities(StorageNodeStatus status) {
        if (status.capabilities().isEmpty()) {
            return "-";
        }
        return status.capabilities().stream().sorted().collect(Collectors.joining(","));
    }

    private void printMetrics(NodeMetricsSnapshot snapshot, PrintStream out) {
        out.println("NODE: " + snapshot.nodeId());
        out.println("LEADER: " + snapshot.leader());
        out.println("SERIES: " + snapshot.seriesCount());
        out.println("USED_BYTES: " + snapshot.usedBytes());
        out.println("CAPACITY_BYTES: " + snapshot.capacityBytes());
        out.println("OPEN_HANDLES: " + snapshot.openHandles());
        out.println("WRITE_BATCHES: " + snapshot.writeBatches());
        out.println("SAMPLES_WRITTEN: " + snapshot.samplesWritten());
        out.println("SAMPLES_FAILED: " + snapshot.samplesFailed());
        out.println("CHECKPOINTS: " + snapshot.checkpoints());
        out.println("READS: " + snapshot.reads());
        out.println("MIGRATIONS_IN: " + snapshot.migrationsIn());
        out.println("MIGRATIONS_OUT: " + snapshot.migrationsOut());
        out.println("RECONCILE_ADOPTED: " + snapshot.reconcileAdopted());
        out.println("RECONCILE_ORPHANS_DELETED: " + snapshot.reconcileOrphansDeleted());
        out.println("RECONCILE_UNPLACED: " + snapshot.reconcileUnplaced());
        out.println("RECONCILE_MISSING: " + snapshot.reconcileMissing());
        out.println("REDIRECT_CONFIRMATIONS: " + snapshot.redirectConfirmations());
        out.println("REDIRECT_OVERRIDES: " + snapshot.redirectOverrides());
        out.println("REDIRECT_CONFIRMATION_FAILURES: " + snapshot.redirectConfirmationFailures());
        out.println("REDIRECT_CACHE_HITS: " + snapshot.redirectCacheHits());
    }

    /**
     * Confirmação do disparo com as contagens do líder e uma linha por destino excluído pela réplica do
     * catálogo (issue #177), em ordem de {@code nodeId}; só a confirmação quando as contagens são
     * desconhecidas.
     */
    private void printRebalance(RebalanceTrigger trigger, PrintStream out) {
        if (!trigger.countsKnown()) {
            out.println("rebalanceamento disparado");
            return;
        }
        out.println("rebalanceamento disparado: planejados=" + trigger.planned() + " iniciados=" + trigger.started());
        new TreeMap<>(trigger.excludedDestinations()).forEach((nodeId, reason) ->
                out.println("destino excluído: " + nodeId + " (" + reason + ")"));
    }

    /** Código de saída 1 quando algum storage não confirmou: o operador precisa repetir o comando nele. */
    private int printForget(AdminForgetResponse response, PrintStream out, PrintStream err) {
        out.println("forget-node OK: " + response.nodeId() + " removido do catálogo pelo líder "
                + response.leaderNodeId());
        for (String nodeId : response.forgottenOn()) {
            out.println("esquecido em: " + nodeId);
        }
        if (response.failedOn().isEmpty()) {
            return 0;
        }
        for (String nodeId : response.failedOn()) {
            err.println("NAO confirmado em: " + nodeId);
        }
        err.println("erro: repita 'forget-node " + response.nodeId() + "' quando esses storages voltarem; "
                + "até lá continuam a contar o nó na maioria de votantes");
        return 1;
    }

    private void printNodeStatus(String command, StorageNodeStatus status, PrintStream out) {
        out.println(command + " OK");
        out.printf(Locale.ROOT, "%-24s %-10s %8s %14s%n", "NODE", "STATE", "SERIES", "BYTES");
        out.printf(Locale.ROOT, "%-24s %-10s %8d %14d%n", status.nodeId(), status.state(), status.seriesCount(),
                status.usedBytes());
    }

    /** Argumentos já parseados e validados de {@link #run(String[], PrintStream, PrintStream)}. */
    private record ParsedArgs(String seed, String clientId, String command, String nodeId) {

        private static final List<String> NODE_ID_COMMANDS = List.of("metrics", "drain", "activate", "forget-node");

        static ParsedArgs parse(String[] args) {
            if (args == null) {
                args = new String[0];
            }
            String seed = null;
            String clientId = null;
            List<String> positional = new ArrayList<>();
            int i = 0;
            while (i < args.length) {
                String arg = args[i];
                if ("--seed".equals(arg)) {
                    seed = requireValue(args, i + 1, "--seed");
                    i += 2;
                } else if ("--client-id".equals(arg)) {
                    clientId = requireValue(args, i + 1, "--client-id");
                    i += 2;
                } else {
                    positional.add(arg);
                    i += 1;
                }
            }
            if (seed == null || seed.isBlank()) {
                throw new IllegalArgumentException("--seed é obrigatório");
            }
            if (positional.isEmpty()) {
                throw new IllegalArgumentException(
                        "comando é obrigatório: status|metrics|drain|activate|forget-node|rebalance");
            }
            String command = positional.get(0).toLowerCase(Locale.ROOT);
            String nodeId = positional.size() > 1 ? positional.get(1) : null;
            if (NODE_ID_COMMANDS.contains(command) && (nodeId == null || nodeId.isBlank())) {
                throw new IllegalArgumentException("comando '" + command + "' exige <nodeId>");
            }
            String resolvedClientId = clientId != null && !clientId.isBlank()
                    ? clientId
                    : "ngrrd-cluster-admin-" + UUID.randomUUID().toString().substring(0, 8);
            return new ParsedArgs(seed, resolvedClientId, command, nodeId);
        }

        private static String requireValue(String[] args, int index, String flag) {
            if (index >= args.length) {
                throw new IllegalArgumentException(flag + " exige um valor");
            }
            return args[index];
        }
    }
}
