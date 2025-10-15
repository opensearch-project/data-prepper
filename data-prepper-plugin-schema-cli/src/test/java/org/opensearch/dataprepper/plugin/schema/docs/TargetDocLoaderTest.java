package org.opensearch.dataprepper.plugin.schema.docs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TargetDocLoaderTest {

    @TempDir
    Path tempDir;

    private TargetDocLoader loader;
    private Path targetDocsPath;

    @BeforeEach
    void setUp() throws Exception {
        targetDocsPath = tempDir.resolve("docs");
        Files.createDirectories(targetDocsPath.resolve("pipelines/configuration/processors"));
        loader = new TargetDocLoader(targetDocsPath);
    }

    @Test
    void loadTemplate_withExistingDoc_loadsTemplate() throws Exception {
        // Create target doc
        Path docPath = targetDocsPath.resolve("pipelines/configuration/processors/test.md");
        Files.writeString(docPath,
            "---\n" +
            "title: Test\n" +
            "---\n\n" +
            "# Test processor\n\n" +
            "## Configuration\n\n" +
            "Configuration content\n\n" +
            "## Examples\n\n" +
            "Examples content\n");

        // Load template
        TargetDocLoader.DocTemplate template = loader.loadTemplate("processors", "test");

        // Verify
        assertNotNull(template);
        assertEquals("---\ntitle: Test\n---\n\n", template.getFrontmatter());
        assertEquals(3, template.getSections().size()); // Title, Config, Examples

        List<MarkdownSection> sections = template.getSections();
        assertEquals("Test processor", sections.get(0).getHeading());
        assertEquals("Configuration", sections.get(1).getHeading());
        assertEquals("Examples", sections.get(2).getHeading());
    }

    @Test
    void loadTemplate_withNonExistentDoc_returnsNull() {
        assertNull(loader.loadTemplate("processors", "non-existent"));
    }

    @Test
    void loadTemplate_withMetricSections_parsesSections() throws Exception {
        // Create target doc
        Path docPath = targetDocsPath.resolve("pipelines/configuration/processors/test.md");
        Files.writeString(docPath,
            "---\n" +
            "title: Test\n" +
            "---\n\n" +
            "# Test processor\n\n" +
            "## Metrics\n\n" +
            "Common metrics\n\n" +
            "### Counter\n\n" +
            "Counter metrics\n\n" +
            "### Timer\n\n" +
            "Timer metrics\n");

        // Load template
        TargetDocLoader.DocTemplate template = loader.loadTemplate("processors", "test");

        // Verify
        assertNotNull(template);
        List<MarkdownSection> sections = template.getSections();
        assertEquals(4, sections.size());

        assertEquals("Test processor", sections.get(0).getHeading());
        assertEquals("Metrics", sections.get(1).getHeading());
        assertEquals("Counter", sections.get(2).getHeading());
        assertEquals("Timer", sections.get(3).getHeading());

        assertEquals(MarkdownSection.SectionType.METRICS, sections.get(1).getType());
        assertEquals(MarkdownSection.SectionType.METRICS, sections.get(2).getType());
        assertEquals(MarkdownSection.SectionType.METRICS, sections.get(3).getType());
    }

    @Test
    void loadTemplate_withMalformedDoc_returnsNull() throws Exception {
        // Create malformed target doc
        Path docPath = targetDocsPath.resolve("pipelines/configuration/processors/test.md");
        Files.writeString(docPath, "Invalid content without frontmatter");

        assertNull(loader.loadTemplate("processors", "test"));
    }
}