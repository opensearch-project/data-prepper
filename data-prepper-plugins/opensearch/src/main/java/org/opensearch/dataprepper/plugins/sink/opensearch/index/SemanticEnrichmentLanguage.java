/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;

public enum SemanticEnrichmentLanguage {
    ENGLISH("english"),
    MULTILINGUAL("multilingual");

    private final String value;

    SemanticEnrichmentLanguage(final String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static SemanticEnrichmentLanguage fromValue(final String value) {
        return Arrays.stream(values())
                .filter(lang -> lang.value.equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid semantic enrichment language: " + value + ". Valid values are: english, multilingual"));
    }
}
