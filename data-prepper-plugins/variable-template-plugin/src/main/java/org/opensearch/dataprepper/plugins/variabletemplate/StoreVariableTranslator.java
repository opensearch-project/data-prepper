/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoreVariableTranslator implements PluginConfigValueTranslator {

    static final String PREFIX = "store";

    private final Map<String, String> store;

    public StoreVariableTranslator(final List<String> sources) {
        this.store = loadSources(sources);
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    public Object translate(final String key) {
        final String value = store.get(key);
        if (value == null) {
            throw new IllegalArgumentException(
                    String.format("Key '%s' not found in variable store.", key));
        }
        return value;
    }

    @Override
    public PluginConfigVariable translateToPluginConfigVariable(final String key) {
        return new ImmutablePluginConfigVariable(translate(key));
    }

    private Map<String, String> loadSources(final List<String> sources) {
        final Map<String, String> result = new HashMap<>();
        for (final String source : sources) {
            result.putAll(parseEnvFile(source));
        }
        return result;
    }

    private Map<String, String> parseEnvFile(final String path) {
        final List<String> lines;
        try {
            lines = Files.readAllLines(Path.of(path));
        } catch (final NoSuchFileException e) {
            throw new IllegalArgumentException(
                    String.format("Store source file not found: '%s'.", path), e);
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to read store source file '%s': %s", path, e.getMessage()), e);
        }

        final Map<String, String> entries = new HashMap<>();
        int lineNumber = 0;
        for (final String rawLine : lines) {
            lineNumber++;
            final String line = rawLine.strip();

            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            final int separatorIndex = line.indexOf('=');
            if (separatorIndex < 1) {
                throw new IllegalArgumentException(
                        String.format(
                        "Invalid entry in store file '%s' at line %d: '%s'. Expected KEY=VALUE.", path, lineNumber, rawLine));
            }

            final String key = line.substring(0, separatorIndex).strip();
            final String rawValue = line.substring(separatorIndex + 1);
            entries.put(key, unquote(rawValue.strip()));
        }
        return entries;
    }

    private String unquote(final String value) {
        if (value.length() >= 2) {
            if ((value.startsWith("\"") && value.endsWith("\""))
                    || (value.startsWith("'") && value.endsWith("'"))) {
                return value.substring(1, value.length() - 1);
            }
        }
        final int commentIndex = findInlineComment(value);
        if (commentIndex >= 0) {
            return value.substring(0, commentIndex).stripTrailing();
        }
        return value;
    }

    private int findInlineComment(final String value) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == '#' && (i == 0 || value.charAt(i - 1) == ' ')) {
                return i;
            }
        }
        return -1;
    }
}
