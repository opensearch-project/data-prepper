package org.opensearch.dataprepper.plugin.schema.docs;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.dataprepper.plugin.schema.docs.generators.ConfigurationTableGenerator;
import org.opensearch.dataprepper.plugin.schema.docs.generators.FrontMatterGenerator;
import org.opensearch.dataprepper.plugin.schema.docs.generators.MetricsTableGenerator;
import org.opensearch.dataprepper.plugin.schema.docs.model.PluginTypeForDocGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Generates markdown documentation from JSON schema and examples.
 */
public class MarkdownGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(MarkdownGenerator.class);

    private final FrontMatterGenerator frontMatterGenerator;
    private final ConfigurationTableGenerator configTableGenerator;
    private final MetricsTableGenerator metricsTableGenerator;
    private final MarkdownDocMerger docMerger;

    public MarkdownGenerator(final Path targetDocsPath) {
        this.frontMatterGenerator = new FrontMatterGenerator();
        this.configTableGenerator = new ConfigurationTableGenerator();
        this.metricsTableGenerator = new MetricsTableGenerator();
        this.docMerger = new MarkdownDocMerger(targetDocsPath);
    }

    /**
     * Generate markdown documentation for a plugin.
     *
     * @param pluginTypeForDocGen The type of plugin
     * @param pluginName The name of the plugin
     * @param schema The JSON schema for the plugin
     * @param examples Map of example names to example content
     * @return Generated markdown content
     */
    public String generateMarkdown(final PluginTypeForDocGen pluginTypeForDocGen,
                                 final String pluginName,
                                 final JsonNode schema,
                                 final Map<String, String> examples) {
        LOG.info("Generating markdown for {} {} plugin", pluginTypeForDocGen, pluginName);

        // Generate initial markdown sections
        final List<MarkdownSection> sections = generateSections(pluginTypeForDocGen, pluginName, schema, examples);

        // Get target template sections
        final TargetDocLoader.DocTemplate template = docMerger.getTargetDocLoader()
            .loadTemplate(pluginTypeForDocGen.toString().toLowerCase(), pluginName);

        if (template != null) {
            // Merge with target template
            return docMerger.mergeSections(
                template.getFrontmatter(),
                template.getSections(),
                sections
            );
        } else {
            // No template - just join sections
            final StringBuilder md = new StringBuilder();
            md.append(frontMatterGenerator.generate(pluginTypeForDocGen, pluginName, schema));
            for (MarkdownSection section : sections) {
                md.append("\n").append(section.toMarkdown());
            }
            return md.toString();
        }
    }

    /**
     * Generate initial markdown sections.
     */
    private List<MarkdownSection> generateSections(final PluginTypeForDocGen pluginTypeForDocGen,
                                                 final String pluginName,
                                                 final JsonNode schema,
                                                 final Map<String, String> examples) {
        final List<MarkdownSection> sections = new ArrayList<>();

        // Add title section
        sections.add(MarkdownSection.createTitle(
            capitalize(pluginName),
            pluginTypeForDocGen.toString().toLowerCase()
        ));

        // Add description under title if available
        if (schema.has("description")) {
            sections.add(new MarkdownSection(
                null, // No heading
                schema.get("description").asText().replaceAll("<code>([^<]+)</code>", "`$1`"),
                sections.size(),
                MarkdownSection.SectionType.CUSTOM
            ));
        }

        // Add configuration section
        sections.add(new MarkdownSection(
            "Configuration",
            configTableGenerator.generate(schema),
            sections.size(),
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

            sections.add(new MarkdownSection(
                "Examples",
                examplesContent.toString(),
                sections.size(),
                MarkdownSection.SectionType.EXAMPLES
            ));
        }

        // Add metrics section for processors
        if (pluginTypeForDocGen == PluginTypeForDocGen.PROCESSOR) {
            sections.add(new MarkdownSection(
                "Metrics",
                metricsTableGenerator.generate(schema),
                sections.size(),
                MarkdownSection.SectionType.METRICS
            ));
        }

        // Add conditional processing section if applicable
        if (schema.has("conditional") && schema.get("conditional").asBoolean()) {
            sections.add(new MarkdownSection(
                "Conditional Processing",
                "This plugin supports conditional processing using expression syntax. " +
                    "See [Expression Syntax]({{site.url}}{{site.baseurl}}/data-prepper/pipelines/expression-syntax/) " +
                    "for more information.",
                sections.size(),
                MarkdownSection.SectionType.CUSTOM
            ));
        }

        return sections;
    }

    private String capitalize(final String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }
}