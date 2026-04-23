package org.opensearch.dataprepper.plugin.schema.docs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.plugin.schema.docs.model.PluginTypeForDocGen;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

class MarkdownGeneratorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    private MarkdownGenerator generator;
    private Path targetDocsPath;

    @BeforeEach
    void setUp() throws Exception {
        targetDocsPath = tempDir.resolve("docs");
        Files.createDirectories(targetDocsPath.resolve("pipelines/configuration/processors"));
        generator = new MarkdownGenerator(targetDocsPath);
    }

    @Test
    void generateMarkdown_forProcessor_includesMetrics() {
        // Create schema
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("description", "Test processor description");

        // Generate
        String markdown = generator.generateMarkdown(
            PluginTypeForDocGen.PROCESSOR,
            "test",
            schema,
            new HashMap<>()
        );

        // Verify
        assertThat(markdown, containsString("# Test processor"));
        assertThat(markdown, containsString("Test processor description"));
        assertThat(markdown, containsString("## Metrics"));
    }

    @Test
    void generateMarkdown_forSource_excludesMetrics() {
        // Create schema
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("description", "Test source description");

        // Generate
        String markdown = generator.generateMarkdown(
            PluginTypeForDocGen.SOURCE,
            "test",
            schema,
            new HashMap<>()
        );

        // Verify
        assertThat(markdown, containsString("# Test source"));
        assertThat(markdown, containsString("Test source description"));
        assertThat(markdown, not(containsString("## Metrics")));
    }

    @Test
    void generateMarkdown_withExamples_includesExamplesSection() {
        // Create schema
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("description", "Test description");

        // Create examples
        Map<String, String> examples = new HashMap<>();
        examples.put("Example 1", "example:\n  key: value");

        // Generate
        String markdown = generator.generateMarkdown(
            PluginTypeForDocGen.PROCESSOR,
            "test",
            schema,
            examples
        );

        // Verify
        assertThat(markdown, containsString("## Examples"));
        assertThat(markdown, containsString("### Example 1"));
        assertThat(markdown, containsString("```yaml"));
        assertThat(markdown, containsString("example:\n  key: value"));
        assertThat(markdown, containsString("```"));
    }

    @Test
    void generateMarkdown_withConditional_includesConditionSection() {
        // Create schema
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("description", "Test description");
        schema.put("conditional", true);

        // Generate
        String markdown = generator.generateMarkdown(
            PluginTypeForDocGen.PROCESSOR,
            "test",
            schema,
            new HashMap<>()
        );

        // Verify
        assertThat(markdown, containsString("## Conditional Processing"));
        assertThat(markdown, containsString("[Expression Syntax]"));
    }

    @Test
    void generateMarkdown_withConfiguration_includesConfigSection() {
        // Create schema
        ObjectNode schema = MAPPER.createObjectNode();
        ObjectNode properties = schema.putObject("properties");
        ObjectNode property = properties.putObject("test_property");
        property.put("type", "string");
        property.put("description", "Test property description");

        // Generate
        String markdown = generator.generateMarkdown(
            PluginTypeForDocGen.PROCESSOR,
            "test",
            schema,
            new HashMap<>()
        );

        // Verify
        assertThat(markdown, containsString("## Configuration"));
        assertThat(markdown, containsString("|:---"));
        assertThat(markdown, containsString("`test_property`"));
        assertThat(markdown, containsString("Test property description"));
    }
}