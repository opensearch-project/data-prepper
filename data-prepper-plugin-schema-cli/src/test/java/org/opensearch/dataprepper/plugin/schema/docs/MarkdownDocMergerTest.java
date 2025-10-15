package org.opensearch.dataprepper.plugin.schema.docs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MarkdownDocMergerTest {
    @TempDir
    Path tempDir;

    private MarkdownDocMerger merger;
    private Path targetDocsPath;

    @BeforeEach
    void setUp() throws Exception {
        targetDocsPath = tempDir.resolve("docs");
        Files.createDirectories(targetDocsPath.resolve("pipelines/configuration/processors"));
        merger = new MarkdownDocMerger(targetDocsPath);
    }

    @Test
    void mergeSections_withNoTemplate_usesGeneratedContent() {
        // Create generated sections
        List<MarkdownSection> generatedSections = new ArrayList<>();
        generatedSections.add(MarkdownSection.createTitle("Test", "processor"));
        generatedSections.add(new MarkdownSection(
            "Configuration",
            "Test configuration content",
            1,
            MarkdownSection.SectionType.CONFIGURATION
        ));

        // Create target sections (empty)
        List<MarkdownSection> targetSections = new ArrayList<>();

        // Merge
        String result = merger.mergeSections(
            "---\nfrontmatter\n---\n",
            targetSections,
            generatedSections
        );

        // Verify
        assertThat(result, containsString("# Test processor"));
        assertThat(result, containsString("## Configuration"));
        assertThat(result, containsString("Test configuration content"));
        assertThat(result, containsString("---\nfrontmatter\n---"));
    }

    @Test
    void mergeSections_withTemplate_prefersGeneratedForConfigSection() {
        // Create generated sections
        List<MarkdownSection> generatedSections = new ArrayList<>();
        generatedSections.add(MarkdownSection.createTitle("Test", "processor"));
        generatedSections.add(new MarkdownSection(
            "Configuration",
            "Generated configuration content",
            1,
            MarkdownSection.SectionType.CONFIGURATION
        ));

        // Create target sections
        List<MarkdownSection> targetSections = new ArrayList<>();
        targetSections.add(new MarkdownSection(
            "Configuration",
            "Target configuration content",
            1,
            MarkdownSection.SectionType.CONFIGURATION
        ));
        targetSections.add(new MarkdownSection(
            "Metrics",
            "Target metrics content",
            2,
            MarkdownSection.SectionType.METRICS
        ));

        // Merge
        String result = merger.mergeSections(
            "---\nfrontmatter\n---\n",
            targetSections,
            generatedSections
        );

        // Verify
        assertThat(result, containsString("Generated configuration content"));
        assertThat(result, not(containsString("Target configuration content")));
        assertThat(result, containsString("Target metrics content"));
    }

    @Test
    void mergeSections_withTemplate_keepsSectionOrder() {
        // Create generated sections
        List<MarkdownSection> generatedSections = new ArrayList<>();
        generatedSections.add(MarkdownSection.createTitle("Test", "processor"));
        generatedSections.add(new MarkdownSection(
            "Examples",
            "Generated examples content",
            1,
            MarkdownSection.SectionType.EXAMPLES
        ));
        generatedSections.add(new MarkdownSection(
            "Configuration",
            "Generated configuration content",
            2,
            MarkdownSection.SectionType.CONFIGURATION
        ));

        // Create target sections with different order
        List<MarkdownSection> targetSections = new ArrayList<>();
        targetSections.add(new MarkdownSection(
            "Configuration",
            "Target configuration content",
            1,
            MarkdownSection.SectionType.CONFIGURATION
        ));
        targetSections.add(new MarkdownSection(
            "Examples",
            "Target examples content",
            2,
            MarkdownSection.SectionType.EXAMPLES
        ));
        targetSections.add(new MarkdownSection(
            "Metrics",
            "Target metrics content",
            3,
            MarkdownSection.SectionType.METRICS
        ));

        // Merge
        String result = merger.mergeSections(
            "---\nfrontmatter\n---\n",
            targetSections,
            generatedSections
        );

        // Verify order
        int configPos = result.indexOf("## Configuration");
        int examplesPos = result.indexOf("## Examples");
        int metricsPos = result.indexOf("## Metrics");

        assertTrue(configPos < examplesPos);
        assertTrue(examplesPos < metricsPos);
    }

    @Test
    void mergeSections_withTemplate_handlesCustomMetrics() {
        // Create generated sections
        List<MarkdownSection> generatedSections = new ArrayList<>();
        generatedSections.add(MarkdownSection.createTitle("Test", "processor"));
        generatedSections.add(new MarkdownSection(
            "Metrics",
            "Common metrics content",
            1,
            MarkdownSection.SectionType.METRICS
        ));

        // Create target sections with custom metrics
        List<MarkdownSection> targetSections = new ArrayList<>();
        targetSections.add(new MarkdownSection(
            "Counter",
            "Counter metrics content",
            1,
            MarkdownSection.SectionType.METRICS
        ));
        targetSections.add(new MarkdownSection(
            "Timer",
            "Timer metrics content",
            2,
            MarkdownSection.SectionType.METRICS
        ));

        // Merge
        String result = merger.mergeSections(
            "---\nfrontmatter\n---\n",
            targetSections,
            generatedSections
        );

        // Verify
        assertThat(result, containsString("Common metrics content"));
        assertThat(result, containsString("Counter metrics content"));
        assertThat(result, containsString("Timer metrics content"));
    }
}