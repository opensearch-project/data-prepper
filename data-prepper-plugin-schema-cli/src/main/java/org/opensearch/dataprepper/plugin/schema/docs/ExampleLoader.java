package org.opensearch.dataprepper.plugin.schema.docs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Loads example YAML files for a plugin.
 */
public class ExampleLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleLoader.class);

    private final Path examplesPath;

    public ExampleLoader(final Path examplesPath) {
        this.examplesPath = examplesPath;
    }

    /**
     * Load all example YAML files from the examples directory.
     *
     * @return Map of example names to example content
     */
    public Map<String, String> loadExamples() {
        final Map<String, String> examples = new HashMap<>();

        if (!Files.exists(examplesPath)) {
            LOG.debug("No examples directory found at {}", examplesPath);
            return examples;
        }

        try (Stream<Path> paths = Files.list(examplesPath)) {
            paths.filter(path -> path.toString().endsWith(".yaml"))
                .sorted()
                .forEach(path -> {
                    try {
                        final String content = Files.readString(path);
                        final String name = getExampleName(path);
                        examples.put(name, content.trim());
                        LOG.debug("Loaded example {} for path {}", name, path);
                    } catch (IOException e) {
                        LOG.error("Error reading example file {}", path, e);
                    }
                });
        } catch (IOException e) {
            LOG.error("Error listing example files in {}", examplesPath, e);
        }

        return examples;
    }

    private String getExampleName(final Path path) {
        final String filename = path.getFileName().toString();
        final String nameWithoutExtension = filename.substring(0, filename.lastIndexOf('.'));

        // Convert names like "example_1" and "basic_example" to proper titles
        final String withoutPrefix = nameWithoutExtension.replaceFirst("^example_", "")
            .replaceFirst("^basic_", "");
        return toTitleCase(withoutPrefix.replace('_', ' ').trim());
    }

    private String toTitleCase(final String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }

        StringBuilder titleCase = new StringBuilder();
        boolean nextTitleCase = true;

        for (char c : str.toLowerCase().toCharArray()) {
            if (Character.isSpaceChar(c) || c == '-' || c == '_') {
                nextTitleCase = true;
            } else if (nextTitleCase) {
                c = Character.toTitleCase(c);
                nextTitleCase = false;
            }
            titleCase.append(c);
        }

        return titleCase.toString();
    }

    private String capitalize(final String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }
}