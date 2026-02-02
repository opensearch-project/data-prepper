package org.opensearch.dataprepper.plugin.schema.docs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugin.schema.docs.model.PluginTypeForDocGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Main class for generating plugin documentation from JSON schemas and examples.
 */
public class PluginDocumentationGenerator {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(PluginDocumentationGenerator.class);

    private final Path schemaDirectory;
    private final Path examplesDirectory;
    private final Path outputDirectory;
    private final Path targetDocsDirectory;
    private final MarkdownGenerator markdownGenerator;

    public PluginDocumentationGenerator(final String schemaDir,
                                      final String examplesDir,
                                      final String outputDir) {
        this(schemaDir, examplesDir, outputDir,
            "/Users/gandheaz/opensource-projects/san81/documentation-website/_data-prepper");
    }

    public PluginDocumentationGenerator(final String schemaDir,
                                      final String examplesDir,
                                      final String outputDir,
                                      final String targetDocsDir) {
        LOG.info("Initializing PluginDocumentationGenerator with: schemaDir={}, examplesDir={}, outputDir={}, targetDocsDir={}",
            schemaDir, examplesDir, outputDir, targetDocsDir);
        this.schemaDirectory = Paths.get(schemaDir);
        this.examplesDirectory = Paths.get(examplesDir);
        this.outputDirectory = Paths.get(outputDir);
        this.targetDocsDirectory = Paths.get(targetDocsDir);
        this.markdownGenerator = new MarkdownGenerator(targetDocsDirectory);
    }

    /**
     * Generate documentation for all plugin types.
     */
    public void generateAll() {
        LOG.info("Starting generateAll");

        // Create output directories
        try {
            Files.createDirectories(outputDirectory);
            LOG.info("Created output directory: {}", outputDirectory);
        } catch (IOException e) {
            LOG.error("Failed to create output directory: {}", e);
            return;
        }

        // Verify schema directory
        if (!Files.exists(schemaDirectory)) {
            LOG.error("Schema directory does not exist: {}", schemaDirectory);
            return;
        }

        // Verify examples directory
        if (!Files.exists(examplesDirectory)) {
            LOG.warn("Examples directory does not exist: {}", examplesDirectory);
        }

        LOG.info("Using schema directory: {}, exists: {}", schemaDirectory, Files.exists(schemaDirectory));
        LOG.info("Using examples directory: {}, exists: {}", examplesDirectory, Files.exists(examplesDirectory));
        LOG.info("Using output directory: {}, exists: {}", outputDirectory, Files.exists(outputDirectory));

        // List schema directory contents
        LOG.info("Listing schema directory contents:");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(schemaDirectory)) {
            boolean found = false;
            for (Path file : stream) {
                LOG.info("Found file in schema directory: {}, size: {} bytes",
                    file, Files.size(file));
                found = true;
            }
            if (!found) {
                LOG.error("No files found in schema directory: {}", schemaDirectory);
                return;
            }
        } catch (IOException e) {
            LOG.error("Error listing schema directory: {}", e);
            return;
        }

        try {
            generateAllPluginTypes();
        } catch (Exception e) {
            LOG.error("Error generating documentation", e);
            e.printStackTrace();
        }
    }

    public void generateAllPluginTypes() {
        LOG.info("Starting documentation generation for all plugin types");
        LOG.info("Checking if schema directory exists: {}", schemaDirectory);
        LOG.info("Schema directory exists: {}", Files.exists(schemaDirectory));
        LOG.info("Listing contents of schema directory:");
        try {
            Files.list(schemaDirectory).forEach(path -> LOG.info("  {}", path));
        } catch (IOException e) {
            LOG.error("Error listing schema directory", e);
        }

        // Process all plugin types
        for (PluginTypeForDocGen pluginTypeForDocGen : PluginTypeForDocGen.values()) {
            try {
                LOG.info("Processing plugin type: {}", pluginTypeForDocGen);
                LOG.info("Generating documentation for plugin type {} in directory {}", pluginTypeForDocGen, outputDirectory);
                generateForType(pluginTypeForDocGen);
            } catch (Exception e) {
                LOG.error("Error generating documentation for plugin type {}", pluginTypeForDocGen, e);
            }
        }
        LOG.info("Completed documentation generation for all plugin types");
    }

    /**
     * Generate documentation for a specific plugin type.
     *
     * @param pluginTypeForDocGen The type of plugin to generate documentation for
     * @throws IOException if there is an error reading from or writing to files
     */
    public void generateForType(final PluginTypeForDocGen pluginTypeForDocGen) throws IOException {
        // Ensure output directory exists
        final Path pluginTypeOutputDir = outputDirectory.resolve(pluginTypeForDocGen.toString().toLowerCase());
        Files.createDirectories(pluginTypeOutputDir);

        // List and process schema files
        LOG.info("Processing plugin type {} with schema directory {}", pluginTypeForDocGen, schemaDirectory);
        if (!Files.exists(schemaDirectory)) {
            LOG.warn("Schema directory does not exist: {}", schemaDirectory);
            return;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(schemaDirectory, "*.json")) {
            for (Path schemaPath : stream) {
                LOG.info("Processing schema file: {}", schemaPath);
                generateForSchema(pluginTypeForDocGen, schemaPath);
            }
        } catch (IOException e) {
            LOG.error("Error reading schema directory: {}", schemaDirectory, e);
            throw e;
        }
    }

    /**
     * Generate documentation for a specific plugin schema.
     *
     * @param pluginTypeForDocGen The type of plugin
     * @param schemaPath Path to the JSON schema file
     */
    private void generateForSchema(final PluginTypeForDocGen pluginTypeForDocGen, final Path schemaPath) throws IOException {
        LOG.info("Generating documentation for schema file: {}", schemaPath);

        // Validate schema file
        if (!Files.exists(schemaPath)) {
            LOG.error("Schema file does not exist: {}", schemaPath);
            return;
        }

        // Load and validate schema
        JsonNode schema;
        try {
            schema = objectMapper.readTree(schemaPath.toFile());
            LOG.info("Successfully loaded schema from {}", schemaPath);
        } catch (Exception e) {
            LOG.error("Failed to parse schema file {}: {}", schemaPath, e.getMessage());
            return;
        }

        // Get plugin name
        String pluginName;
        try {
            pluginName = getPluginNameFromSchema(schema);
            LOG.info("Using plugin name from schema: {}", pluginName);
        } catch (IllegalArgumentException e) {
            try {
                pluginName = getPluginNameFromPath(schemaPath);
                LOG.info("Using plugin name from path: {}", pluginName);
            } catch (Exception ex) {
                LOG.error("Could not determine plugin name: {}", ex.getMessage());
                return;
            }
        }

        // Create output directories
        Path pluginTypeDir = outputDirectory.resolve(pluginTypeForDocGen.toString().toLowerCase());
        try {
            Files.createDirectories(pluginTypeDir);
            LOG.info("Created output directory: {}", pluginTypeDir);
        } catch (IOException e) {
            LOG.error("Failed to create output directory {}: {}", pluginTypeDir, e.getMessage());
            return;
        }

        // Load examples
        Path examplesTypeDir = examplesDirectory.resolve(pluginTypeForDocGen.toString().toLowerCase());
        Path pluginExamplesDir = examplesTypeDir.resolve(pluginName);
        Map<String, String> examples = new HashMap<>();
        if (Files.exists(pluginExamplesDir)) {
            try {
                examples = new ExampleLoader(pluginExamplesDir).loadExamples();
                LOG.info("Loaded {} examples from {}", examples.size(), pluginExamplesDir);
            } catch (Exception e) {
                LOG.warn("Failed to load examples from {}: {}", pluginExamplesDir, e.getMessage());
            }
        } else {
            LOG.info("No examples found at {}", pluginExamplesDir);
        }

        try {
            // Generate markdown
            String markdown = markdownGenerator.generateMarkdown(pluginTypeForDocGen, pluginName, schema, examples);
            LOG.info("Generated {} characters of markdown", markdown.length());

            // Write markdown file
            Path outputPath = pluginTypeDir.resolve(pluginName + ".md");
            Files.writeString(outputPath, markdown);
            LOG.info("Successfully wrote documentation to {}", outputPath);
        } catch (Exception e) {
            LOG.error("Error generating documentation for schema {}: {}", schemaPath, e);
            throw new IOException("Failed to generate documentation", e);
        }
    }

    private String getPluginNameFromSchema(final JsonNode schema) {
        // Try to get name from schema
        if (schema.has("name")) {
            final String name = schema.get("name").asText();
            LOG.debug("Found plugin name '{}' in schema name field", name);
            return name;
        }

        // Try to get from title
        if (schema.has("title")) {
            final String title = schema.get("title").asText();
            LOG.debug("Found title '{}' in schema", title);
            // Convert "Grok Processor Configuration" to "grok"
            String name = title.toLowerCase()
                .replaceAll("\\s+processor\\s+configuration$", "")
                .replaceAll("\\s+source\\s+configuration$", "")
                .replaceAll("\\s+sink\\s+configuration$", "")
                .replaceAll("\\s+extension\\s+configuration$", "")
                .replaceAll("\\s+", "-");
            LOG.debug("Converted title to plugin name '{}'", name);
            return name;
        }

        throw new IllegalArgumentException("Could not determine plugin name from schema");
    }

    private String getPluginNameFromPath(final Path schemaPath) {
        LOG.debug("Getting plugin name from path {}", schemaPath);
        if (!Files.exists(schemaPath)) {
            throw new IllegalArgumentException("Schema file does not exist: " + schemaPath);
        }
        LOG.debug("Getting plugin name from schema path: {}", schemaPath);
        final String filename = schemaPath.getFileName().toString();
        return filename.substring(0, filename.lastIndexOf('.')).toLowerCase();
    }
}