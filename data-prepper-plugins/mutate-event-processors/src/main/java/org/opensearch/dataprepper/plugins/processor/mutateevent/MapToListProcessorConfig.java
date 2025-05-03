/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.IfThenElse;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.SchemaProperty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;

import java.util.ArrayList;
import java.util.List;

@ConditionalRequired(value = {
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "entries", value = "null")},
                thenExpect = {@SchemaProperty(field = "source")}
        ),
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "source", value = "null")},
                thenExpect = {@SchemaProperty(field = "entries")}
        )
})
@JsonPropertyOrder
@JsonClassDescription("The <code>map_to_list</code> processor converts a map of key-value pairs to a list of objects. " +
        "Each object contains the key and value in separate fields.")
public class MapToListProcessorConfig {
    private static final String DEFAULT_KEY_NAME = "key";
    private static final String DEFAULT_VALUE_NAME = "value";
    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();
    private static final boolean DEFAULT_REMOVE_PROCESSED_FIELDS = false;

    public static class Entry {
        @NotNull
        @JsonProperty("source")
        @JsonPropertyDescription("The source map used to perform the mapping operation. When set to an empty " +
                "string (<code>\"\"</code>), it will use the root of the event as the <code>source</code>.")
        private String source;

        @NotEmpty
        @NotNull
        @JsonProperty("target")
        @JsonPropertyDescription("The target for the generated list.")
        private String target;

        @JsonProperty(value = "key_name", defaultValue = DEFAULT_KEY_NAME)
        @JsonPropertyDescription("The name of the field in which to store the original key. Default is <code>key</code>.")
        @ExampleValues({
                @Example(value = "og_key", description = "The original key in the map is stored in 'og_key' in the list.")
        })
        private String keyName = DEFAULT_KEY_NAME;

        @JsonProperty(value = "value_name", defaultValue = DEFAULT_VALUE_NAME)
        @JsonPropertyDescription("The name of the field in which to store the original value. Default is <code>value</code>.")
        @ExampleValues({
                @Example(value = "og_value", description = "The original value in the map is stored in 'og_value' in the list.")
        })
        private String valueName = DEFAULT_VALUE_NAME;

        @JsonProperty("remove_processed_fields")
        @JsonPropertyDescription("When <code>true</code>, the processor will remove the processed fields from the source map. " +
                "Default is <code>false</code>.")
        private boolean removeProcessedFields = DEFAULT_REMOVE_PROCESSED_FIELDS;

        @JsonProperty("convert_field_to_list")
        @JsonPropertyDescription("If <code>true</code>, the processor will convert the fields from the source map into lists and " +
                "place them in fields in the target list. Default is <code>false</code>.")
        private boolean convertFieldToList = false;

        @JsonProperty("exclude_keys")
        @JsonPropertyDescription("The keys in the source map that will be excluded from processing. Default is an " +
                "empty list (<code>[]</code>).")
        private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

        @JsonProperty("tags_on_failure")
        @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
        private List<String> tagsOnFailure;

        @JsonProperty("map_to_list_when")
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
                "such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will " +
                "be run on the event. By default, all events will be processed unless otherwise stated.")
        @ExampleValues({
                @Example(value = "/some-key == \"test\"", description = "When the key is 'test', the processor will be applied to the event.")
        })
        private String mapToListWhen;

        public String getSource() { return source; }
        public String getTarget() { return target; }
        public String getKeyName() { return keyName; }
        public String getValueName() { return valueName; }
        public boolean getRemoveProcessedFields() { return removeProcessedFields; }
        public boolean getConvertFieldToList() { return convertFieldToList; }
        public List<String> getExcludeKeys() { return excludeKeys; }
        public List<String> getTagsOnFailure() { return tagsOnFailure; }
        public String getMapToListWhen() { return mapToListWhen; }

        public Entry(final String source,
                     final String target,
                     final String keyName,
                     final String valueName,
                     final boolean removeProcessedFields,
                     final boolean convertFieldToList,
                     final List<String> excludeKeys,
                     final List<String> tagsOnFailure,
                     final String mapToListWhen) {
            this.source = source;
            this.target = target;
            this.keyName = keyName;
            this.valueName = valueName;
            this.removeProcessedFields = removeProcessedFields;
            this.convertFieldToList = convertFieldToList;
            this.excludeKeys = excludeKeys;
            this.tagsOnFailure = tagsOnFailure;
            this.mapToListWhen = mapToListWhen;
        }

        public Entry() {

        }
    }

    @JsonProperty("source")
    @JsonPropertyDescription("The source map used to perform the mapping operation. When set to an empty " +
            "string (<code>\"\"</code>), it will use the root of the event as the <code>source</code>.")
    private String source;

    @JsonProperty("target")
    @JsonPropertyDescription("The target for the generated list.")
    private String target;

    @JsonProperty(value = "key_name", defaultValue = DEFAULT_KEY_NAME)
    @JsonPropertyDescription("The name of the field in which to store the original key. Default is <code>key</code>.")
    @ExampleValues({
        @Example(value = "og_key", description = "The original key in the map is stored in 'og_key' in the list.")
    })
    private String keyName = DEFAULT_KEY_NAME;

    @JsonProperty(value = "value_name", defaultValue = DEFAULT_VALUE_NAME)
    @JsonPropertyDescription("The name of the field in which to store the original value. Default is <code>value</code>.")
    @ExampleValues({
        @Example(value = "og_value", description = "The original value in the map is stored in 'og_value' in the list.")
    })
    private String valueName = DEFAULT_VALUE_NAME;

    @JsonProperty("remove_processed_fields")
    @JsonPropertyDescription("When <code>true</code>, the processor will remove the processed fields from the source map. " +
            "Default is <code>false</code>.")
    private boolean removeProcessedFields = DEFAULT_REMOVE_PROCESSED_FIELDS;

    @JsonProperty("convert_field_to_list")
    @JsonPropertyDescription("If <code>true</code>, the processor will convert the fields from the source map into lists and " +
            "place them in fields in the target list. Default is <code>false</code>.")
    private boolean convertFieldToList = false;

    @JsonProperty("exclude_keys")
    @JsonPropertyDescription("The keys in the source map that will be excluded from processing. Default is an " +
            "empty list (<code>[]</code>).")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
    private List<String> tagsOnFailure;

    @JsonProperty("map_to_list_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
            "such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will " +
            "be run on the event. By default, all events will be processed unless otherwise stated.")
    @ExampleValues({
        @Example(value = "/some-key == \"test\"", description = "When the key is 'test', the processor will be applied to the event.")
    })
    private String mapToListWhen;

    @Valid
    @JsonProperty("entries")
    @JsonPropertyDescription("A list of entries to process map-to-list operations.")
    private List<Entry> entries;

    @AssertTrue(message = "Must use either entries configuration or individual configuration")
    boolean isUsingAtLeastOneConfiguration() {
        return entries != null || source != null;
    }

    @AssertTrue(message = "Cannot use both entries configuration and individual configuration together")
    boolean isNotUsingBothConfigurations() {
        return entries == null || source == null;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getKeyName() {
        return keyName;
    }

    public String getValueName() {
        return valueName;
    }

    public String getMapToListWhen() {
        return mapToListWhen;
    }

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public boolean getRemoveProcessedFields() {
        return removeProcessedFields;
    }

    public boolean getConvertFieldToList() {
        return convertFieldToList;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    public List<Entry> getEntries() {
        return entries;
    }
}
