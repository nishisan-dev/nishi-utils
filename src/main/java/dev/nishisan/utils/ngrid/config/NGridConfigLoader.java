package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        throw new IllegalArgumentException("Environment variable or property '" + varName + "' not found and no default value provided.");
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

        dev.nishisan.utils.ngrid.common.NodeId nodeId = nodeConfig.getId() != null ?
                dev.nishisan.utils.ngrid.common.NodeId.of(nodeConfig.getId()) :
                dev.nishisan.utils.ngrid.common.NodeId.randomId();

        dev.nishisan.utils.ngrid.common.NodeInfo localNode = new dev.nishisan.utils.ngrid.common.NodeInfo(
                nodeId,
                nodeConfig.getHost(),
                nodeConfig.getPort(),
                nodeConfig.getRoles()
        );

        dev.nishisan.utils.ngrid.structures.NGridConfig.Builder builder = dev.nishisan.utils.ngrid.structures.NGridConfig.builder(localNode);

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
                    String nodeIdValue = seedNode.getId() != null ? seedNode.getId() : "seed-" + seedNode.getHost() + ":" + seedNode.getPort();
                    builder.addPeer(new dev.nishisan.utils.ngrid.common.NodeInfo(
                            dev.nishisan.utils.ngrid.common.NodeId.of(nodeIdValue),
                            seedNode.getHost(),
                            seedNode.getPort()
                    ));
                }
            } else if (clusterConfig.getSeeds() != null) {
                for (String seed : clusterConfig.getSeeds()) {
                    String[] parts = seed.split(":");
                    if (parts.length == 2) {
                        if (parts[0].equals(nodeConfig.getHost()) && Integer.parseInt(parts[1]) == nodeConfig.getPort()) {
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
                        dev.nishisan.utils.ngrid.common.NodeId seedId = dev.nishisan.utils.ngrid.common.NodeId.of("seed-" + seed); 
                        builder.addPeer(new dev.nishisan.utils.ngrid.common.NodeInfo(seedId, parts[0], Integer.parseInt(parts[1])));
                    }
                }
            } else if (seedInfo != null) {
                builder.addPeer(seedInfo);
            }
        }

        // Queue Policy
        QueuePolicyConfig queueConfig = yamlConfig.getQueue();
        if (queueConfig != null) {
            dev.nishisan.utils.queue.NQueue.Options options = dev.nishisan.utils.queue.NQueue.Options.defaults();
            
            if (queueConfig.getRetention() != null) {
                if ("TIME_BASED".equalsIgnoreCase(queueConfig.getRetention().getPolicy())) {
                    options.withRetentionPolicy(dev.nishisan.utils.queue.NQueue.Options.RetentionPolicy.TIME_BASED);
                    options.withRetentionTime(parseDuration(queueConfig.getRetention().getDuration()));
                } else {
                    options.withRetentionPolicy(dev.nishisan.utils.queue.NQueue.Options.RetentionPolicy.DELETE_ON_CONSUME);
                }
            }

            if (queueConfig.getPerformance() != null) {
                options.withFsync(queueConfig.getPerformance().isFsync());
                options.withShortCircuit(queueConfig.getPerformance().isShortCircuit());
                if (queueConfig.getPerformance().getMemoryBuffer() != null) {
                    options.withMemoryBuffer(queueConfig.getPerformance().getMemoryBuffer().isEnabled());
                    options.withMemoryBufferSize(queueConfig.getPerformance().getMemoryBuffer().getSize());
                }
            }

            if (queueConfig.getCompaction() != null) {
                options.withCompactionWasteThreshold(queueConfig.getCompaction().getThreshold());
                options.withCompactionInterval(parseDuration(queueConfig.getCompaction().getInterval()));
            }
            
            builder.queueOptions(options);
            
            // Default queue directory/name
            builder.queueDirectory(baseDir.resolve("queue"));
            if (queueConfig.getName() != null) {
                builder.queueName(queueConfig.getName());
            }
        } else {
             builder.queueDirectory(baseDir.resolve("queue"));
        }

        // Map Policy (Default/Main map)
        // NGridConfig currently supports one main map via builder.
        // Additional maps from the list will need to be handled by the node after start, 
        // OR we map the first one here if meaningful.
        // For now, we set the map directory.
        builder.mapDirectory(baseDir.resolve("maps"));
        
        // If maps list is present, pick the first one as default or just set directory?
        // Let's set defaults.
        
        return builder.build();
    }

    private static Duration parseDuration(String s) {
        if (s == null || s.isBlank()) return null;
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