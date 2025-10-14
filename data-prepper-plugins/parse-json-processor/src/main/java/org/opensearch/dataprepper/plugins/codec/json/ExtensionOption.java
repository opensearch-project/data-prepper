/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum ExtensionOption {
    NDJSON("ndjson"),
    JSONL("jsonl");

    private final String extension;

    private static final Map<String, ExtensionOption> EXTENSIONS_MAP = Arrays.stream(ExtensionOption.values())
            .collect(Collectors.toMap(
                    ExtensionOption::getExtension,
                    extensionOption -> extensionOption
            ));

    ExtensionOption(String extension) {
        this.extension = extension;
    }

    @JsonValue
    public String getExtension() {
        return extension;
    }

    @JsonCreator
    public static ExtensionOption fromExtension(String extension) {
        return EXTENSIONS_MAP.get(extension.toLowerCase());
    }
}
