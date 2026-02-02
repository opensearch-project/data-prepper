package org.opensearch.dataprepper.plugin.schema.docs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads and parses target documentation files.
 */
public class TargetDocLoader {
    private static final Logger LOG = LoggerFactory.getLogger(TargetDocLoader.class);

    // Regex to match markdown sections (## heading through next ## or end)
    private static final Pattern SECTION_PATTERN = Pattern.compile(
        "(?m)^##\\s+([^\\n]+)\\n(.*?)(?=\\n##|\\z)",
        Pattern.DOTALL
    );

    // Regex to match frontmatter between --- markers
    private static final Pattern FRONTMATTER_PATTERN = Pattern.compile(
        "\\A---\\n(.*?)\\n---\\n",
        Pattern.DOTALL
    );

    private final Path targetDocsPath;

    public TargetDocLoader(final Path targetDocsPath) {
        this.targetDocsPath = targetDocsPath;
    }

    /**
     * Load sections from target documentation.
     *
     * @param pluginType Type of plugin (processor, source, sink, etc)
     * @param pluginName Name of plugin
     * @return List of markdown sections and frontmatter content
     */
    public DocTemplate loadTemplate(final String pluginType, final String pluginName) {
        // Find target doc
        final Path targetDocPath = targetDocsPath
            .resolve("pipelines")
            .resolve("configuration")
            .resolve(pluginType)
            .resolve(pluginName + ".md");

        if (!Files.exists(targetDocPath)) {
            LOG.info("No target doc found at {}", targetDocPath);
            return null;
        }

        try {
            final String content = Files.readString(targetDocPath);
            return parseContent(content);
        } catch (IOException e) {
            LOG.error("Error reading target doc {}", targetDocPath, e);
            return null;
        }
    }

    /**
     * Parse content into sections.
     */
    private DocTemplate parseContent(final String content) {
        final List<MarkdownSection> sections = new ArrayList<>();
        final String frontmatter = extractFrontmatter(content);

        final Matcher matcher = SECTION_PATTERN.matcher(content);
        int order = 0;
        while (matcher.find()) {
            final String heading = matcher.group(1).trim();
            final String sectionContent = matcher.group(2).trim();

            sections.add(new MarkdownSection(
                heading,
                sectionContent,
                order++,
                MarkdownSection.SectionType.fromHeading(heading)
            ));
        }

        return new DocTemplate(frontmatter, sections);
    }

    /**
     * Extract frontmatter from content.
     */
    private String extractFrontmatter(final String content) {
        final Matcher matcher = FRONTMATTER_PATTERN.matcher(content);
        if (matcher.find()) {
            return matcher.group(0);
        }
        return "";
    }

    /**
     * Represents a parsed markdown template.
     */
    public static class DocTemplate {
        private final String frontmatter;
        private final List<MarkdownSection> sections;

        public DocTemplate(final String frontmatter, final List<MarkdownSection> sections) {
            this.frontmatter = frontmatter;
            this.sections = sections;
        }

        public String getFrontmatter() {
            return frontmatter;
        }

        public List<MarkdownSection> getSections() {
            return sections;
        }
    }
}