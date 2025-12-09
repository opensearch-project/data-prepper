package org.opensearch.dataprepper.plugin.schema.docs;

import org.opensearch.dataprepper.plugin.schema.cli.DataPrepperPluginSchemaExecute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

@Command(
    name = "generate-docs",
    description = "Generate plugin documentation from JSON schemas and examples"
)
public class DocumentationGeneratorCommand implements Callable<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(DocumentationGeneratorCommand.class);
    private static final String DEFAULT_JSON_SCHEMA_LOCATION = "build/plugin-json-schema";
    private static final String DEFAULT_EXAMPLES_LOCATION = "data-prepper-plugin-schema/examples";
    private static final String DEFAULT_OUTPUT_LOCATION = "build/plugin-docs";

    @Option(
        names = {"--schema-dir"},
        description = "Directory containing JSON schema files",
        required = false,
        defaultValue = DEFAULT_JSON_SCHEMA_LOCATION
    )
    private String schemaDir;

    @Option(
        names = {"--examples-dir"},
        description = "Directory containing example YAML files",
        required = false,
        defaultValue = DEFAULT_EXAMPLES_LOCATION
    )
    private String examplesDir;

    @Option(
        names = {"--output-dir"},
        description = "Directory where generated documentation will be written",
        required = false,
        defaultValue = DEFAULT_OUTPUT_LOCATION
    )
    private String outputDir;

    @Override
    public Integer call() {
        try {
            // Create and validate schema directory
            final Path schemaPath = Paths.get(schemaDir);
            LOG.info("Using schema directory: {}", schemaPath);
            if (!Files.exists(schemaPath)) {
                LOG.error("Schema directory does not exist: {}", schemaPath);
                throw new RuntimeException("Schema directory does not exist: " + schemaPath);
            }

            // Create and validate examples directory
            final Path examplesPath = Paths.get(examplesDir);
            LOG.info("Using examples directory: {}", examplesPath);
            if (!Files.exists(examplesPath)) {
                LOG.warn("Examples directory does not exist: {}", examplesPath);
            }

            // Create output directory
            final Path outputPath = Paths.get(outputDir);
            Files.deleteIfExists(outputPath);
            Files.createDirectories(outputPath);
            LOG.info("Using output directory: {}", outputPath);

            // List schema files
            LOG.debug("Looking for schema files in {}", schemaPath);
            boolean foundSchemaFiles = false;
            try (java.nio.file.DirectoryStream<Path> stream = Files.newDirectoryStream(schemaPath, "*.json")) {
                for (Path file : stream) {
                    LOG.info("Found schema file: {}", file);
                    foundSchemaFiles = true;
                }
            }

            if (!foundSchemaFiles) {
                LOG.error("No JSON schema files found in {}", schemaPath);
                throw new RuntimeException("No JSON schema files found in " + schemaPath);
            }

            // Create documentation generator
            LOG.debug("Creating documentation generator with schema:{} examples:{} output:{}",
                schemaPath, examplesPath, outputPath);
            final PluginDocumentationGenerator generator = new PluginDocumentationGenerator(
                schemaDir,
                examplesDir,
                outputPath.toString()
            );

            // Generate documentation
            LOG.info("Starting documentation generation");
            generator.generateAll();
            LOG.info("Documentation generation completed successfully");

            return 0;
        } catch (Exception e) {
            LOG.error("Error generating documentation", e);
            return 1;
        }
    }

    public static void main(String[] args) {
        String[] schemaArgs = new String[]{"--schema-dir", DEFAULT_JSON_SCHEMA_LOCATION};
        // first generate the schema
        new CommandLine(new DataPrepperPluginSchemaExecute()).execute(schemaArgs);
        // and then generate documentation
        System.exit(new CommandLine(new DocumentationGeneratorCommand()).execute(args));
    }
}