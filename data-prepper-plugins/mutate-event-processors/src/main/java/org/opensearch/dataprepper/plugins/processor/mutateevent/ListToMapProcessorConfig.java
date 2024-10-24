/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonPropertyOrder
@JsonClassDescription("The <code>list_to_map</code> processor converts a list of objects from an event, " +
        "where each object contains a <code>key</code> field, into a map of target keys.")
public class ListToMapProcessorConfig {
    public enum FlattenedElement {
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

        @JsonValue
        public String getOptionValue() {
            return name;
        }
    }

    @NotEmpty
    @NotNull
    @JsonProperty("source")
    @JsonPropertyDescription("The list of objects with <code>key</code> fields to be converted into keys for the generated map.")
    private String source;

    @JsonProperty("target")
    @JsonPropertyDescription("The target for the generated map. When not specified, the generated map will be " +
            "placed in the root node.")
    private String target = null;

    @JsonProperty("use_source_key")
    @JsonPropertyDescription("When <code>true</code>, keys in the generated map will use original keys from the source. " +
            "Default is <code>false</code>.")
    private boolean useSourceKey = false;

    @JsonProperty("key")
    @JsonPropertyDescription("The key of the fields to be extracted as keys in the generated mappings. Must be " +
            "specified if <code>use_source_key</code> is <code>false</code>.")
    private String key;

    @JsonProperty("value_key")
    @JsonPropertyDescription("When specified, values given a <code>value_key</code> in objects contained in the source list " +
            "will be extracted and converted into the value specified by this option based on the generated map. " +
            "When not specified, objects contained in the source list retain their original value when mapped.")
    private String valueKey = null;

    @JsonProperty("extract_value")
    @JsonPropertyDescription("When <code>true</code>, object values from the source list will be extracted and added to " +
            "the generated map. When <code>false</code>, object values from the source list are added to the generated map " +
            "as they appear in the source list. Default is <code>false</code>")
    private boolean extractValue = false;

    @NotNull
    @JsonProperty("flatten")
    @JsonPropertyDescription("When <code>true</code>, values in the generated map output flatten into single items based on " +
            "the <code>flattened_element</code>. Otherwise, objects mapped to values from the generated map appear as lists. " +
            "Default is <code>false</code>.")
    private boolean flatten = false;

    @NotNull
    @JsonProperty(value = "flattened_element", defaultValue = "first")
    @JsonPropertyDescription("The element to keep, either <code>first</code> or <code>last</code>, when <code>flatten</code> is set to <code>true</code>.")
    private FlattenedElement flattenedElement = FlattenedElement.FIRST;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
    private List<String> tagsOnFailure;

    @JsonProperty("list_to_map_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
            "such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will be " +
            "run on the event. By default, all events will be processed unless otherwise stated.")
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

    public boolean getUseSourceKey() {
        return useSourceKey;
    }

    public boolean getExtractValue() {
        return extractValue;
    }

    public boolean getFlatten() {
        return flatten;
    }

    public String getListToMapWhen() { return listToMapWhen; }

    public FlattenedElement getFlattenedElement() {
        return flattenedElement;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}
