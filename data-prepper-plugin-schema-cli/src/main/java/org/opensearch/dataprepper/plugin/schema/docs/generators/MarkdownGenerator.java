package org.opensearch.dataprepper.plugin.schema.docs.generators;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.dataprepper.plugin.schema.docs.MarkdownSection;
import org.opensearch.dataprepper.plugin.schema.docs.model.PluginTypeForDocGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates markdown documentation from JSON schema and examples.
 */
public class MarkdownGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(MarkdownGenerator.class);

    // Default section order
    private static final List<String> SECTION_ORDER = List.of(
        "Title",
        "Configuration",
        "Usage",
        "Examples",
        "Conditional Processing",
        "Performance Metadata",
        "Metrics"
    );

    private final FrontMatterGenerator frontMatterGenerator;
    private final ConfigurationTableGenerator configTableGenerator;
    private final MetricsTableGenerator metricsTableGenerator;

    public MarkdownGenerator() {
        this.frontMatterGenerator = new FrontMatterGenerator();
        this.configTableGenerator = new ConfigurationTableGenerator();
        this.metricsTableGenerator = new MetricsTableGenerator();
    }

    /**
     * Generate markdown documentation for a plugin.
     *
     * @param pluginTypeForDocGen The type of plugin
     * @param pluginName The name of the plugin
     * @param schema The JSON schema for the plugin
     * @param examples Map of example names to example content
     * @return List of markdown sections
     */
    public List<MarkdownSection> generateSections(final PluginTypeForDocGen pluginTypeForDocGen,
                                              final String pluginName,
                                              final JsonNode schema,
                                              final Map<String, String> examples) {
        LOG.info("Generating markdown for {} {} plugin", pluginTypeForDocGen, pluginName);

        // Create ordered sections map
        Map<String, MarkdownSection> sections = new LinkedHashMap<>();

        // Add title section first
        sections.put("Title", MarkdownSection.createTitle(
            capitalize(pluginName),
            pluginTypeForDocGen.toString().toLowerCase() + " processor"
        ));

        // Add description under title if available
        if (schema.has("description")) {
            sections.get("Title").appendContent("\n\n" + schema.get("description").asText());
        }

        // Add configuration section
        sections.put("Configuration", new MarkdownSection(
            "Configuration",
            configTableGenerator.generate(schema),
            -1, // Order set later
            MarkdownSection.SectionType.CONFIGURATION
        ));

        // Add examples section if available
        if (!examples.isEmpty()) {
            LOG.debug("Adding {} examples", examples.size());
            final StringBuilder examplesContent = new StringBuilder();

            examples.forEach((name, content) -> {
                examplesContent.append("### ").append(name).append("\n\n");
                examplesContent.append("```yaml\n");
                examplesContent.append(content).append("\n");
                examplesContent.append("```\n");
                examplesContent.append("{% include copy.html %}\n\n");
            });

            sections.put("Examples", new MarkdownSection(
                "Examples",
                examplesContent.toString(),
                -1, // Order set later
                MarkdownSection.SectionType.EXAMPLES
            ));
        }

        // Add metrics section for processors
        if (pluginTypeForDocGen == PluginTypeForDocGen.PROCESSOR) {
            sections.put("Metrics", new MarkdownSection(
                "Metrics",
                metricsTableGenerator.generate(schema),
                -1, // Order set later
                MarkdownSection.SectionType.METRICS
            ));
        }

        // Add conditional processing section if applicable
        if (schema.has("conditional") && schema.get("conditional").asBoolean()) {
            sections.put("Conditional Processing", new MarkdownSection(
                "Conditional Processing",
                "This plugin supports conditional processing using expression syntax. " +
                    "See [Expression Syntax]({{site.url}}{{site.baseurl}}/data-prepper/pipelines/expression-syntax/) " +
                    "for more information.",
                -1, // Order set later
                MarkdownSection.SectionType.CUSTOM
            ));
        }

        // Convert to ordered list
        List<MarkdownSection> orderedSections = new ArrayList<>();
        int order = 0;

        // Add sections in standard order first
        for (String sectionName : SECTION_ORDER) {
            if (sections.containsKey(sectionName)) {
                MarkdownSection section = sections.get(sectionName);
                section.setOrder(order++);
                orderedSections.add(section);
                sections.remove(sectionName);
            }
        }

        // Add any remaining sections
        for (MarkdownSection section : sections.values()) {
            section.setOrder(order++);
            orderedSections.add(section);
        }

        return orderedSections;
    }

    private String capitalize(final String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }
}