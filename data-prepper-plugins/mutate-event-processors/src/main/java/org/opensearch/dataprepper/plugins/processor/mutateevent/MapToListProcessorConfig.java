/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>map_to_list</code> processor converts a map of key-value pairs to a list of objects. " +
        "Each object contains the key and value in separate fields.")
public class MapToListProcessorConfig {
    private static final String DEFAULT_KEY_NAME = "key";
    private static final String DEFAULT_VALUE_NAME = "value";
    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();
    private static final boolean DEFAULT_REMOVE_PROCESSED_FIELDS = false;

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

    @JsonProperty("key_name")
    @JsonPropertyDescription("The name of the field in which to store the original key. Default is <code>key</code>.")
    private String keyName = DEFAULT_KEY_NAME;

    @JsonProperty("value_name")
    @JsonPropertyDescription("The name of the field in which to store the original value. Default is <code>value</code>.")
    private String valueName = DEFAULT_VALUE_NAME;

    @JsonProperty("map_to_list_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
            "such as <code>/some-key == \"test\"'</code>, that will be evaluated to determine whether the processor will " +
            "be run on the event. By default, all events will be processed unless otherwise stated.")
    private String mapToListWhen;

    @JsonProperty("exclude_keys")
    @JsonPropertyDescription("The keys in the source map that will be excluded from processing. Default is an " +
            "empty list (<code>[]</code>).")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("remove_processed_fields")
    @JsonPropertyDescription("When <code>true</code>, the processor will remove the processed fields from the source map. " +
            "Default is <code>false</code>.")
    private boolean removeProcessedFields = DEFAULT_REMOVE_PROCESSED_FIELDS;

    @JsonProperty("convert_field_to_list")
    @JsonPropertyDescription("If <code>true</code>, the processor will convert the fields from the source map into lists and " +
            "place them in fields in the target list. Default is <code>false</code>.")
    private boolean convertFieldToList = false;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
    private List<String> tagsOnFailure;

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
}
