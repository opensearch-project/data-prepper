package org.opensearch.dataprepper.plugin.schema.docs.generators;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generates markdown tables for plugin metrics documentation.
 */
public class MetricsTableGenerator {

    private static final String ABSTRACT_PROCESSOR_LINK =
        "[Abstract processor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/processor/AbstractProcessor.java)";

    /**
     * Generate metrics documentation including both common and custom metrics.
     *
     * @param schema JSON schema that may contain metrics information
     * @return Generated markdown section for metrics
     */
    public String generate(final JsonNode schema) {
        final List<String> sections = new ArrayList<>();

        // Add common metrics section
        sections.add(generateCommonMetricsSection());

        // Add custom metrics if defined
        final JsonNode metrics = schema.get("metrics");
        if (metrics != null && metrics.isObject() && metrics.size() > 0) {
            sections.add("\nThis plugin includes the following custom metrics.\n");

            // Group metrics by type
            Map<String, List<String>> metricsByType = new TreeMap<>(); // TreeMap for consistent ordering

            metrics.fields().forEachRemaining(field -> {
                final String metricName = field.getKey();
                final JsonNode metric = field.getValue();
                final String type = metric.path("type").asText("");
                final String description = metric.path("description")
                    .asText("No description available.")
                    .replaceAll("<code>([^<]+)</code>", "`$1`"); // Replace HTML code tags with backticks

                final String entry = String.format("* `%s`: %s", metricName, description);

                metricsByType.computeIfAbsent(type, k -> new ArrayList<>()).add(entry);
            });

            // Add each type section
            if (metricsByType.containsKey("counter")) {
                sections.add("\n### Counter\n");
                sections.addAll(metricsByType.get("counter"));
            }

            if (metricsByType.containsKey("timer")) {
                sections.add("\n### Timer\n");
                sections.addAll(metricsByType.get("timer"));
            }

            // Add any other types
            metricsByType.forEach((type, entries) -> {
                if (!type.equals("counter") && !type.equals("timer") && !entries.isEmpty()) {
                    sections.add(String.format("\n### %s Metrics\n", capitalize(type)));
                    sections.addAll(entries);
                }
            });
        }

        return String.join("\n", sections);
    }

    /**
     * Generate common metrics section.
     */
    private String generateCommonMetricsSection() {
        return String.format(
            "The following table describes common %s metrics.\n\n" +
            "| Metric name | Type | Description |\n" +
            "| ------------- | ---- | -----------|\n" +
            "| `recordsIn` | Counter | Metric representing the ingress of records to a pipeline component. |\n" +
            "| `recordsOut` | Counter | Metric representing the egress of records from a pipeline component. |\n" +
            "| `timeElapsed` | Timer | Metric representing the time elapsed during execution of a pipeline component. |",
            ABSTRACT_PROCESSOR_LINK
        );
    }

    private String capitalize(final String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}