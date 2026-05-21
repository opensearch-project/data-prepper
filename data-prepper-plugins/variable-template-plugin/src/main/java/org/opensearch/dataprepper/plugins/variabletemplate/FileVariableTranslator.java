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

public class FileVariableTranslator implements PluginConfigValueTranslator {

    static final String PREFIX = "file";

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    public Object translate(final String path) {
        try {
            return Files.readString(Path.of(path)).strip();
        } catch (final NoSuchFileException e) {
            throw new IllegalArgumentException(
                    String.format("Secret file not found: '%s'.", path), e);
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to read secret file '%s': %s", path, e.getMessage()), e);
        }
    }

    @Override
    public PluginConfigVariable translateToPluginConfigVariable(final String path) {
        return new ImmutablePluginConfigVariable(translate(path));
    }
}
