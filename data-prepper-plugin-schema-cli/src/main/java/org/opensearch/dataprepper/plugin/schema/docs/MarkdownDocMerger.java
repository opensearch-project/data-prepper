package org.opensearch.dataprepper.plugin.schema.docs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Merges generated markdown content with target documentation templates.
 */
public class MarkdownDocMerger {
    private static final Logger LOG = LoggerFactory.getLogger(MarkdownDocMerger.class);

    // Regex to match markdown sections (any level # through next # or end)
    private static final Pattern SECTION_PATTERN = Pattern.compile(
        "(?m)^(#+)\\s+([^\\n]+)\\n(.*?)(?=\\n#+|\\z)",
        Pattern.DOTALL
    );

    // Regex to match frontmatter between --- markers
    private static final Pattern FRONTMATTER_PATTERN = Pattern.compile(
        "\\A---\\n(.*?)\\n---\\n",
        Pattern.DOTALL
    );

    private final TargetDocLoader targetDocLoader;

    public MarkdownDocMerger(final Path targetDocsPath) {
        this.targetDocLoader = new TargetDocLoader(targetDocsPath);
    }

    /**
     * Get the target doc loader.
     */
    public TargetDocLoader getTargetDocLoader() {
        return targetDocLoader;
    }

    /**
     * Merge target and generated sections into final markdown.
     */
    public String mergeSections(
            final String frontMatter,
            final List<MarkdownSection> targetSections,
            final List<MarkdownSection> generatedSections) {

        // Create map of generated sections by heading for easy lookup
        final Map<String, MarkdownSection> generatedSectionMap = new HashMap<>();
        for (MarkdownSection section : generatedSections) {
            if (section.getHeading() != null) {
                generatedSectionMap.put(section.getHeading(), section);
            }
        }

        // Build merged content
        final List<String> mergedParts = new ArrayList<>();

        // Add frontmatter
        mergedParts.add(frontMatter);

        // Add title section first
        MarkdownSection titleSection = generatedSections.get(0); // First section is always title
        mergedParts.add(titleSection.toMarkdown());

        // Process each section in target order, skipping title since we added it
        for (MarkdownSection templateSection : targetSections) {
            if (templateSection.getType() == MarkdownSection.SectionType.TITLE) {
                continue;
            }

            MarkdownSection sectionToUse = templateSection;

            // If this section type prefers generated content and we have generated content,
            // use the generated version
            if (templateSection.getType().shouldPreferGenerated() &&
                generatedSectionMap.containsKey(templateSection.getHeading())) {
                sectionToUse = generatedSectionMap.get(templateSection.getHeading());
                generatedSectionMap.remove(templateSection.getHeading());
            }

            mergedParts.add(sectionToUse.toMarkdown());
        }

        // Add any remaining generated sections that weren't in the template
        for (MarkdownSection remainingSection : generatedSectionMap.values()) {
            if (remainingSection.getType() != MarkdownSection.SectionType.TITLE) {
                mergedParts.add(remainingSection.toMarkdown());
            }
        }

        return String.join("\n", mergedParts);
    }

    /**
     * Parse markdown content into sections.
     */
    public List<MarkdownSection> parseSections(final String content) {
        final List<MarkdownSection> sections = new ArrayList<>();
        final Matcher matcher = SECTION_PATTERN.matcher(content);

        int order = 0;
        while (matcher.find()) {
            final String heading = matcher.group(2).trim();
            final String sectionContent = matcher.group(3).trim();
            final int level = matcher.group(1).length(); // Number of # characters

            sections.add(new MarkdownSection(
                heading,
                sectionContent,
                order++,
                MarkdownSection.SectionType.fromHeading(heading),
                level
            ));
        }

        return sections;
    }

    /**
     * Extract frontmatter from markdown content.
     */
    private String extractFrontmatter(final String content) {
        final Matcher matcher = FRONTMATTER_PATTERN.matcher(content);
        if (matcher.find()) {
            return matcher.group(0);
        }
        return "";
    }
}