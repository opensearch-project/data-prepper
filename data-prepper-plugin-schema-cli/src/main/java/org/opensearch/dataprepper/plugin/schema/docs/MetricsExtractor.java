package org.opensearch.dataprepper.plugin.schema.docs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Extracts metrics information from plugin source code and target docs.
 */
public class MetricsExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsExtractor.class);

    // Regex patterns for extracting metrics from source code
    private static final Pattern METRICS_CONSTANT = Pattern.compile(
        "static final String ([A-Z_]+) = \"([^\"]+)\";"
    );

    private static final Pattern METRICS_COMMENT = Pattern.compile(
        "/\\*\\*\\s*([^*]|\\*(?!/))*?\\*/",
        Pattern.DOTALL
    );

    // Standard metric types
    private static final List<String> COUNTER_SUFFIXES = Arrays.asList(
        "Counter", "Count", "Total", "Errors", "Failures", "Timeouts"
    );

    private static final List<String> TIMER_SUFFIXES = Arrays.asList(
        "Timer", "Time", "Duration", "Latency"
    );

    /**
     * Extract metrics information from plugin class and target docs.
     *
     * @param pluginClass The plugin class to extract metrics from
     * @param targetDocsPath Path to target documentation
     * @param schema The schema to add metrics to
     */
    public void extractMetrics(Class<?> pluginClass, Path targetDocsPath, ObjectNode schema) {
        final Map<String, Map<String, String>> metrics = new HashMap<>();

        // Extract metrics from source code
        extractMetricsFromSource(pluginClass, metrics);

        // Extract metrics from target docs if available
        if (targetDocsPath != null) {
            extractMetricsFromDocs(targetDocsPath, metrics);
        }

        // Add metrics to schema
        if (!metrics.isEmpty()) {
            final ObjectNode metricsNode = schema.putObject("metrics");
            metrics.forEach((name, props) -> {
                final ObjectNode metricNode = metricsNode.putObject(name);
                props.forEach(metricNode::put);
            });
        }
    }

    /**
     * Extract metrics information from source code.
     */
    private void extractMetricsFromSource(Class<?> pluginClass, Map<String, Map<String, String>> metrics) {
        // Get source file content
        try {
            final String source = new String(Files.readAllBytes(
                Path.of(pluginClass.getResource(pluginClass.getSimpleName() + ".class").getPath()
                    .replace("build/classes/java/main", "src/main/java")
                    .replace(".class", ".java"))
            ));

            // Extract metric constants
            final Matcher constantMatcher = METRICS_CONSTANT.matcher(source);
            while (constantMatcher.find()) {
                final String name = constantMatcher.group(2);
                metrics.put(name, new HashMap<>());

                // Try to determine type from name
                String type = determineMetricType(name);
                if (type != null) {
                    metrics.get(name).put("type", type);
                }

                // Look for associated comment
                final String precedingText = source.substring(0, constantMatcher.start());
                final Matcher commentMatcher = METRICS_COMMENT.matcher(precedingText);
                String lastComment = null;
                while (commentMatcher.find()) {
                    lastComment = commentMatcher.group();
                }
                if (lastComment != null) {
                    metrics.get(name).put("description",
                        lastComment.replaceAll("/\\*+\\s*", "")
                            .replaceAll("\\s*\\*/", "")
                            .replaceAll("\\s+", " ")
                            .trim()
                    );
                }
            }
        } catch (IOException e) {
            LOG.warn("Could not read source file for {}", pluginClass.getName(), e);
        }
    }

    /**
     * Extract metrics information from target documentation.
     */
    private void extractMetricsFromDocs(Path targetDocsPath, Map<String, Map<String, String>> metrics) {
        try {
            final String content = Files.readString(targetDocsPath);

            // Find metrics sections
            Pattern metricsSection = Pattern.compile(
                "### (Counter|Timer|Other Metrics)[\\s\\S]*?(?=###|$)"
            );
            Matcher sectionMatcher = metricsSection.matcher(content);

            while (sectionMatcher.find()) {
                String section = sectionMatcher.group();
                String type = sectionMatcher.group(1).toLowerCase();

                // Extract metrics from section
                Pattern metricPattern = Pattern.compile(
                    "\\* `([^`]+)`:\\s*([^\\n]+)"
                );
                Matcher metricMatcher = metricPattern.matcher(section);

                while (metricMatcher.find()) {
                    String name = metricMatcher.group(1);
                    String description = metricMatcher.group(2).trim();

                    // Add or update metric info
                    metrics.computeIfAbsent(name, k -> new HashMap<>());
                    metrics.get(name).put("type", type);
                    metrics.get(name).put("description", description);
                }
            }
        } catch (IOException e) {
            LOG.warn("Could not read target docs at {}", targetDocsPath, e);
        }
    }

    /**
     * Try to determine metric type from its name.
     */
    private String determineMetricType(String name) {
        String upperName = name.toUpperCase();

        if (COUNTER_SUFFIXES.stream().anyMatch(suffix -> upperName.endsWith(suffix.toUpperCase()))) {
            return "counter";
        }

        if (TIMER_SUFFIXES.stream().anyMatch(suffix -> upperName.endsWith(suffix.toUpperCase()))) {
            return "timer";
        }

        return null;
    }
}