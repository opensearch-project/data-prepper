/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonAlias;
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.IfThenElse;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.SchemaProperty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.typeconverter.ConverterArguments;

import java.util.List;
import java.util.Optional;

@ConditionalRequired(value = {
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "key", value = "null")},
                thenExpect = {@SchemaProperty(field = "keys")}
        ),
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "keys", value = "null")},
                thenExpect = {@SchemaProperty(field = "key")}
        )
})
@JsonPropertyOrder
@JsonClassDescription("The <code>convert_type</code> processor converts a value associated with the specified key in " +
        "a event to the specified type. It is a casting processor that changes the types of specified fields in events.")
public class ConvertEntryTypeProcessorConfig implements ConverterArguments {
    static final String KEY_KEY = "key";
    static final String KEYS_KEY = "keys";

    @JsonProperty(KEY_KEY)
    @JsonPropertyDescription("Key whose value needs to be converted to a different type. Cannot be declared at the same time as <code>keys</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = KEYS_KEY, allowedValues = {"null"})
    })
    private String key;

    @JsonProperty(KEYS_KEY)
    @JsonPropertyDescription("List of keys whose values needs to be converted to a different type. Cannot be declared at the same time as <code>key</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = KEY_KEY, allowedValues = {"null"})
    })
    private List<String> keys;

    @JsonProperty(value = "type", defaultValue = "integer")
    @JsonPropertyDescription("Target type for the values. Default value is <code>integer</code>.")
    private TargetType type = TargetType.INTEGER;

    @JsonProperty("null_conversion_values")
    @JsonAlias("null_values")
    @JsonPropertyDescription("String representation of what constitutes a null value. If the field value equals one of these strings, then the value is considered null and is converted to null.")
    private List<String> nullValues;

    /**
     * Optional scale value used only in the case of BigDecimal converter
     */
    @JsonProperty(value = "scale", defaultValue = "0")
    @JsonPropertyDescription("Modifies the scale of the <code>big_decimal</code> when converting to a <code>big_decimal</code>. The default value is 0.")
    private int scale = 0;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to be added to the event metadata when the event fails to convert.")
    private List<String> tagsOnFailure;

    @JsonProperty("convert_when")
    @JsonPropertyDescription("Specifies a condition using a <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> for performing the <code>convert_type</code> operation. If specified, the <code>convert_type</code> operation runs only when the expression evaluates to true. Example: <code>/mykey != \"test\"</code>")
    @ExampleValues({
        @Example(value = "/some_key == null", description = "Only runs the convert_type processor on the Event if the existing key some_key is null or does not exist."),
        @Example(value = "/some_key typeof integer", description = "Only runs the convert_type processor on the Event if the key some_key is an integer.")
    })
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
