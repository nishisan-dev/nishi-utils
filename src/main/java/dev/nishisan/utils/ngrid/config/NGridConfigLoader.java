package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dev.nishisan.utils.ngrid.map.MapPersistenceMode;
public class NGridConfigLoader {

    private static final ObjectMapper mapper;

    static {
        // Configure mapper to minimize quotes in YAML for cleaner output
        YAMLFactory yamlFactory = new YAMLFactory()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
        mapper = new ObjectMapper(yamlFactory);
    }

    public static NGridYamlConfig load(Path yamlFile) throws IOException {
        return load(yamlFile, System::getenv);
    }

    public static NGridYamlConfig load(Path yamlFile, Function<String, String> envProvider) throws IOException {
        String content = Files.readString(yamlFile);
        String processedContent = resolveVariables(content, envProvider);
        return mapper.readValue(processedContent, NGridYamlConfig.class);
    }

    private static String resolveVariables(String content, Function<String, String> envProvider) {
        // Regex handles ${VAR} and ${VAR:defaultValue}
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(content);
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            String replacement = getReplacement(matcher.group(1), envProvider);
            builder.append(content, i, matcher.start());
            if (replacement == null) {
                builder.append(matcher.group(0));
            } else {
                builder.append(replacement);
            }
            i = matcher.end();
        }
        builder.append(content.substring(i));
        return builder.toString();
    }

    private static String getReplacement(String group, Function<String, String> envProvider) {
        String[] parts = group.split(":", 2);
        String varName = parts[0];
        String defaultValue = parts.length > 1 ? parts[1] : null;

        String value = envProvider.apply(varName);
        if (value != null) {
            return value;
        }
        if (defaultValue != null) {
            return defaultValue;
        }
        throw new IllegalArgumentException(
                "Environment variable or property '" + varName + "' not found and no default value provided.");
    }

    public static void save(Path yamlFile, NGridYamlConfig config) throws IOException {
        mapper.writeValue(yamlFile.toFile(), config);
    }

    public static dev.nishisan.utils.ngrid.structures.NGridConfig convertToDomain(NGridYamlConfig yamlConfig) {
        return convertToDomain(yamlConfig, null);
    }

    public static dev.nishisan.utils.ngrid.structures.NGridConfig convertToDomain(NGridYamlConfig yamlConfig,
            dev.nishisan.utils.ngrid.common.NodeInfo seedInfo) {
        NodeIdentityConfig nodeConfig = yamlConfig.getNode();
        if (nodeConfig == null) {
            throw new IllegalArgumentException("Node configuration is missing");
        }

        dev.nishisan.utils.ngrid.common.NodeId nodeId = nodeConfig.getId() != null
                ? dev.nishisan.utils.ngrid.common.NodeId.of(nodeConfig.getId())
                : dev.nishisan.utils.ngrid.common.NodeId.randomId();

        dev.nishisan.utils.ngrid.common.NodeInfo localNode = new dev.nishisan.utils.ngrid.common.NodeInfo(
                nodeId,
                nodeConfig.getHost(),
                nodeConfig.getPort(),
                nodeConfig.getRoles());

        dev.nishisan.utils.ngrid.structures.NGridConfig.Builder builder = dev.nishisan.utils.ngrid.structures.NGridConfig
                .builder(localNode);

        // Base Directory
        Path baseDir = null;
        if (nodeConfig.getDirs() != null && nodeConfig.getDirs().getBase() != null) {
            baseDir = Path.of(nodeConfig.getDirs().getBase());
        }

        // Cluster Policy
        ClusterPolicyConfig clusterConfig = yamlConfig.getCluster();
        if (clusterConfig != null) {
            if (clusterConfig.getName() != null) {
                builder.clusterName(clusterConfig.getName());
            }
            if (clusterConfig.getReplication() != null) {
                builder.replicationFactor(clusterConfig.getReplication().getFactor());
                builder.strictConsistency(clusterConfig.getReplication().isStrict());
            }
            if (clusterConfig.getTransport() != null) {
                builder.transportWorkerThreads(clusterConfig.getTransport().getWorkers());
            }
            if (clusterConfig.getSeedNodes() != null && !clusterConfig.getSeedNodes().isEmpty()) {
                for (ClusterPolicyConfig.SeedNodeConfig seedNode : clusterConfig.getSeedNodes()) {
                    if (seedNode.getHost() == null || seedNode.getHost().isBlank()) {
                        continue;
                    }
                    if (seedNode.getHost().equals(nodeConfig.getHost()) && seedNode.getPort() == nodeConfig.getPort()) {
                        continue;
                    }
                    String nodeIdValue = seedNode.getId() != null ? seedNode.getId()
                            : "seed-" + seedNode.getHost() + ":" + seedNode.getPort();
                    builder.addPeer(new dev.nishisan.utils.ngrid.common.NodeInfo(
                            dev.nishisan.utils.ngrid.common.NodeId.of(nodeIdValue),
                            seedNode.getHost(),
                            seedNode.getPort()));
                }
            } else if (clusterConfig.getSeeds() != null) {
                for (String seed : clusterConfig.getSeeds()) {
                    String[] parts = seed.split(":");
                    if (parts.length == 2) {
                        if (parts[0].equals(nodeConfig.getHost())
                                && Integer.parseInt(parts[1]) == nodeConfig.getPort()) {
                            continue;
                        }
                        if (seedInfo != null
                                && parts[0].equals(seedInfo.host())
                                && Integer.parseInt(parts[1]) == seedInfo.port()) {
                            builder.addPeer(seedInfo);
                            continue;
                        }
                        // For seed peers, we create a temporary NodeInfo.
                        // Ideally, discovery will update the NodeId later.
                        // We use a temporary placeholder ID for the seed.
                        dev.nishisan.utils.ngrid.common.NodeId seedId = dev.nishisan.utils.ngrid.common.NodeId
                                .of("seed-" + seed);
                        builder.addPeer(new dev.nishisan.utils.ngrid.common.NodeInfo(seedId, parts[0],
                                Integer.parseInt(parts[1])));
                    }
                }
            } else if (seedInfo != null) {
                builder.addPeer(seedInfo);
            }
        }

        // Queue Policy - Support both single queue (legacy) and multiple queues (new)
        java.util.List<QueuePolicyConfig> queueConfigs = yamlConfig.getQueues();

        // Backward compatibility: if queues array is empty, check for legacy single
        // queue
        if (queueConfigs == null || queueConfigs.isEmpty()) {
            QueuePolicyConfig singleQueue = yamlConfig.getQueue();
            if (singleQueue != null) {
                queueConfigs = java.util.List.of(singleQueue);
            }
        }

        // Convert and add all queues
        if (queueConfigs != null && !queueConfigs.isEmpty()) {
            for (QueuePolicyConfig qpc : queueConfigs) {
                builder.addQueue(convertQueuePolicyToQueueConfig(qpc));
            }
            // Set data directory for queue storage
            if (baseDir != null) {
                builder.dataDirectory(baseDir);
            }
        } else {
            // No queues configured - just set data directory
            if (baseDir != null) {
                builder.dataDirectory(baseDir);
            }
        }

        // Map Policy (Default/Main map)
        // NGridConfig currently supports one main map via builder.
        // Additional maps from the list will need to be handled by the node after
        // start,
        // OR we map the first one here if meaningful.
        // For now, we set the map directory.
        builder.mapDirectory(baseDir.resolve("maps"));

        // If maps list is present, pick the first one as default or just set directory?
        // Let's set defaults.
        List<MapPolicyConfig> mapConfigs = yamlConfig.getMaps();
        if (mapConfigs != null && !mapConfigs.isEmpty()) {
            for (MapPolicyConfig mapConfig : mapConfigs) {
                if (mapConfig == null) {
                    continue;
                }
                String persistence = mapConfig.getPersistence();
                if (persistence != null && !persistence.isBlank()) {
                    String normalized = persistence.trim().toUpperCase();
                    try {
                        builder.mapPersistenceMode(MapPersistenceMode.valueOf(normalized));
                    } catch (IllegalArgumentException ignored) {
                        // Ignore invalid values and keep defaults
                    }
                    break;
                }
            }
        }

        return builder.build();
    }

    /**
     * Converts a YAML QueuePolicyConfig to a domain QueueConfig.
     */
    private static dev.nishisan.utils.ngrid.structures.QueueConfig convertQueuePolicyToQueueConfig(
            QueuePolicyConfig qpc) {
        if (qpc == null || qpc.getName() == null) {
            throw new IllegalArgumentException("Queue configuration must have a name");
        }

        dev.nishisan.utils.ngrid.structures.QueueConfig.Builder builder = dev.nishisan.utils.ngrid.structures.QueueConfig
                .builder(qpc.getName());

        // Convert Retention Policy
        if (qpc.getRetention() != null && qpc.getRetention().getPolicy() != null) {
            String policy = qpc.getRetention().getPolicy().trim().toUpperCase();

            if ("TIME_BASED".equals(policy)) {
                Duration duration = parseDuration(qpc.getRetention().getDuration());
                if (duration != null) {
                    builder.retention(
                            dev.nishisan.utils.ngrid.structures.QueueConfig.RetentionPolicy.timeBased(duration));
                }
            } else if ("DELETE_ON_CONSUME".equals(policy)) {
                throw new IllegalArgumentException(
                        "Retention policy DELETE_ON_CONSUME is not supported for NGrid YAML queues");
            } else if ("SIZE_BASED".equals(policy)) {
                // Note: QueuePolicyConfig doesn't have a maxSize field yet
                // For now, we'll use a default time-based policy
                // TODO: Add maxSize field to QueuePolicyConfig.RetentionConfig if needed
                builder.retention(
                        dev.nishisan.utils.ngrid.structures.QueueConfig.RetentionPolicy.timeBased(Duration.ofDays(7)));
            } else if ("COUNT_BASED".equals(policy)) {
                // Note: QueuePolicyConfig doesn't have a maxItems field yet
                // For now, we'll use a default time-based policy
                // TODO: Add maxItems field to QueuePolicyConfig.RetentionConfig if needed
                builder.retention(
                        dev.nishisan.utils.ngrid.structures.QueueConfig.RetentionPolicy.timeBased(Duration.ofDays(7)));
            }
        }

        // Convert Performance settings to NQueue.Options
        if (qpc.getPerformance() != null) {
            dev.nishisan.utils.queue.NQueue.Options options = dev.nishisan.utils.queue.NQueue.Options.defaults();

            options.withFsync(qpc.getPerformance().isFsync());
            options.withShortCircuit(qpc.getPerformance().isShortCircuit());

            if (qpc.getPerformance().getMemoryBuffer() != null) {
                options.withMemoryBuffer(qpc.getPerformance().getMemoryBuffer().isEnabled());
                options.withMemoryBufferSize(qpc.getPerformance().getMemoryBuffer().getSize());
            }

            if (qpc.getCompaction() != null) {
                options.withCompactionWasteThreshold(qpc.getCompaction().getThreshold());
                Duration compactionInterval = parseDuration(qpc.getCompaction().getInterval());
                if (compactionInterval != null) {
                    options.withCompactionInterval(compactionInterval);
                }
            }

            builder.nqueueOptions(options);
        }

        return builder.build();
    }

    private static Duration parseDuration(String s) {
        if (s == null || s.isBlank())
            return null;
        s = s.trim().toUpperCase();
        try {
            return Duration.parse(s); // Try standard ISO-8601 first (PT10M)
        } catch (java.time.format.DateTimeParseException e) {
            // Fallback for simple "10m", "2h", "30s"
            if (s.endsWith("H")) {
                return Duration.ofHours(Long.parseLong(s.substring(0, s.length() - 1)));
            } else if (s.endsWith("M")) {
                return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
            } else if (s.endsWith("S")) {
                return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
            } else if (s.endsWith("MS")) {
                return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
            }
            throw e;
        }
    }
}
