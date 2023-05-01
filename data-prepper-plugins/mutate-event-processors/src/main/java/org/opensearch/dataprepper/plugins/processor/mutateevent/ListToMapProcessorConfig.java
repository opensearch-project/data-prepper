/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class ListToMapProcessorConfig {
    enum FlattenedElement {
        FIRST("first"),
        LAST("last");

        private final String name;

        private static final Map<String, FlattenedElement> ACTIONS_MAP = Arrays.stream(FlattenedElement.values())
                .collect(Collectors.toMap(
                        value -> value.name,
                        value -> value
                ));

        FlattenedElement(String name) {
            this.name = name.toLowerCase();
        }

        @JsonCreator
        static FlattenedElement fromOptionValue(final String option) {
            return ACTIONS_MAP.get(option);
        }
    }

    @NotEmpty
    @NotNull
    @JsonProperty("source")
    private String source;

    @JsonProperty("target")
    private String target = null;

    @NotEmpty
    @NotNull
    @JsonProperty("key")
    private String key;

    @JsonProperty("value_key")
    private String valueKey = null;

    @NotNull
    @JsonProperty("flatten")
    private boolean flatten = false;

    @NotNull
    @JsonProperty("flattened_element")
    private FlattenedElement flattenedElement = FlattenedElement.FIRST;

    @JsonProperty("list_to_map_when")
    private String listToMapWhen;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getKey() {
        return key;
    }

    public String getValueKey() {
        return valueKey;
    }

    public boolean getFlatten() {
        return flatten;
    }

    public String getListToMapWhen() { return listToMapWhen; }

    public FlattenedElement getFlattenedElement() {
        return flattenedElement;
    }
}
