/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.file;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An enumm to represent the file formats supported in Data Prepper's file source.
 * @since 1.2
 */
public enum FileFormat {

    PLAIN("plain"),
    JSON("json");

    private static final Map<String, FileFormat> NAMES_MAP = Arrays.stream(FileFormat.values())
            .collect(Collectors.toMap(FileFormat::toString, Function.identity()));

    private final String name;

    FileFormat(final String name) {
        this.name = name;
    }

    public String toString() {
        return this.name;
    }

    public static FileFormat getByName(final String name) {
        return NAMES_MAP.get(name.toLowerCase());
    }
}