/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.opensearch.dataprepper.typeconverter.ConverterArguments;

import java.util.List;
import java.util.Optional;

@JsonPropertyOrder
@JsonClassDescription("The <code>convert_entry_type</code> processor converts a value associated with the specified key in " +
        "a event to the specified type. It is a casting processor that changes the types of specified fields in events.")
public class ConvertEntryTypeProcessorConfig implements ConverterArguments {
    @JsonProperty("key")
    @JsonPropertyDescription("Key whose value needs to be converted to a different type.")
    private String key;

    @JsonProperty("keys")
    @JsonPropertyDescription("List of keys whose values needs to be converted to a different type.")
    private List<String> keys;

    @JsonProperty("type")
    @JsonPropertyDescription("Target type for the values. Default value is <code>integer.</code>")
    private TargetType type = TargetType.INTEGER;

    @JsonProperty("null_values")
    @JsonPropertyDescription("String representation of what constitutes a null value. If the field value equals one of these strings, then the value is considered null and is converted to null.")
    private List<String> nullValues;

    /**
     * Optional scale value used only in the case of BigDecimal converter
     */
    @JsonProperty("scale")
    @JsonPropertyDescription("Modifies the scale of the <code>big_decimal</code> when converting to a <code>big_decimal</code>. The default value is 0.")
    private int scale = 0;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to be added to the event metadata when the event fails to convert.")
    private List<String> tagsOnFailure;

    @JsonProperty("convert_when")
    @JsonPropertyDescription("Specifies a condition using a <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> for performing the <code>convert_entry_type</code> operation. If specified, the <code>convert_entry_type</code> operation runs only when the expression evaluates to true. Example: <code>/mykey != \"---\"</code>")
    private String convertWhen;

    public String getKey() {
        return key;
    }

    public List<String> getKeys() { return keys; }

    public TargetType getType() { return type;  }

    @Override
    public int getScale() { return scale;  }

    public String getConvertWhen() { return convertWhen; }

    public Optional<List<String>> getNullValues(){
        return Optional.ofNullable(nullValues);
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}
