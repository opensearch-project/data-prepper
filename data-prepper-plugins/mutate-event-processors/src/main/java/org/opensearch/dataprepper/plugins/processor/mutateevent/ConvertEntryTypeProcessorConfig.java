/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.opensearch.dataprepper.typeconverter.ConverterArguments;

import java.util.List;
import java.util.Optional;

public class ConvertEntryTypeProcessorConfig implements ConverterArguments {
    @JsonProperty("key")
    @JsonPropertyDescription("Key whose value needs to be converted to a different type.")
    private String key;

    @JsonProperty("keys")
    @JsonPropertyDescription("List of keys whose value needs to be converted to a different type.")
    private List<String> keys;

    @JsonProperty("type")
    @JsonPropertyDescription("Target type for the key-value pair. Possible values are integer, long, double, big_decimal, string, and boolean. Default value is integer.")
    private TargetType type = TargetType.INTEGER;

    /**
     * Optional scale value used only in the case of BigDecimal converter
     */
    @JsonProperty("scale")
    @JsonPropertyDescription("Modifies the scale of the big_decimal when converting to a big_decimal. The default value is 0.")
    private int scale = 0;

    @JsonProperty("convert_when")
    @JsonPropertyDescription("Specifies a condition using a Data Prepper expression (https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/) for performing the convert_entry_type operation. If specified, the convert_entry_type operation runs only when the expression evaluates to true.")
    private String convertWhen;

    @JsonProperty("null_values")
    @JsonPropertyDescription("String representation of what constitutes a null value. If the field value equals one of these strings, then the value is considered null and is converted to null.")
    private List<String> nullValues;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to be added to the event metadata when the event fails to convert.")
    private List<String> tagsOnFailure;

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
