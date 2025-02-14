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
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.IfThenElse;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.SchemaProperty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;

import java.util.List;
import java.util.stream.Stream;

@ConditionalRequired(value = {
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "key", value = "null")},
                thenExpect = {@SchemaProperty(field = "metadata_key")}
        ),
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "metadata_key", value = "null")},
                thenExpect = {@SchemaProperty(field = "key")}
        ),
        @IfThenElse(
                ifFulfilled = {
                        @SchemaProperty(field = "format", value = "null"),
                        @SchemaProperty(field = "value", value = "null"),
                },
                thenExpect = {@SchemaProperty(field = "value_expression")}
        ),
        @IfThenElse(
                ifFulfilled = {
                        @SchemaProperty(field = "format", value = "null"),
                        @SchemaProperty(field = "value_expression", value = "null"),
                },
                thenExpect = {@SchemaProperty(field = "value")}
        ),
        @IfThenElse(
                ifFulfilled = {
                        @SchemaProperty(field = "value", value = "null"),
                        @SchemaProperty(field = "value_expression", value = "null"),
                },
                thenExpect = {@SchemaProperty(field = "format")}
        )
})
@JsonPropertyOrder
@JsonClassDescription("The <code>add_entries</code> processor adds entries to an event.")
public class AddEntryProcessorConfig {
    static final String VALUE_EXPRESSION_KEY = "value_expression";
    static final String METADATA_KEY_KEY = "metadata_key";
    static final String APPEND_IF_KEY_EXISTS_KEY = "append_if_key_exists";
    static final String OVERWRITE_IF_KEY_EXISTS_KEY = "overwrite_if_key_exists";

    @JsonPropertyOrder
    public static class Entry {
        @JsonPropertyDescription("The key of the new entry to be added. Some examples of keys include <code>my_key</code>, " +
                "<code>myKey</code>, and <code>object/sub_Key</code>. The key can also be a format expression, for example, <code>${/key1}</code> to " +
                "use the value of field <code>key1</code> as the key. Exactly one of <code>key</code> or <code>metadata_key</code> is required.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name=METADATA_KEY_KEY, allowedValues = {"null"})
        })
        @ExampleValues({
                @Example(value = "my_key", description = "Adds 'my_key' to the Event"),
                @Example(value = "${/key_one}-${/key_two}", description = "Evaluates existing Event keys key_one and key_two and adds the result to the Event"),
                @Example(value = "/nested/key", description = "Adds a nested key of { \"nested\": { \"key\": \"some_value\" }"),
        })
        private String key;

        @JsonProperty(METADATA_KEY_KEY)
        @JsonPropertyDescription("The key for the new metadata attribute. The argument must be a literal string key " +
                "and not a JSON Pointer. Adds an attribute to the Events that will not be sent to the sinks, but can be used for condition expressions and routing with the getMetadata function. " +
                "Exactly one of <code>key</code> or <code>metadata_key</code> is required.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name="key", allowedValues = {"null"})
        })
        @ExampleValues({
                @Example(value = "some_metadata", description = "The Event will contain a metadata key called 'some_metadata' that can be used in expressions with sending the key to the sinks.")
        })
        private String metadataKey;

        @JsonPropertyDescription("The value of the new entry to be added, which can be used with any of the " +
                "following data types: strings, Booleans, numbers, null, nested objects, and arrays.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name="format", allowedValues = {"null"}),
                @AlsoRequired.Required(name=VALUE_EXPRESSION_KEY, allowedValues = {"null"})
        })
        @ExampleValues({
                @Example(value = "my_string_value", description = "Adds a value of 'my_string_value' to the key or metadata_key"),
                @Example(value = "false", description = "Adds a value of false to the key or metadata_key"),
                @Example(value = "10", description = "Adds a value of 10 to the key or metadata_key"),
                @Example(value = "[ \"element_one\", \"element_two\" ]", description = "Adds an array value with two elements to the key or metadata_key"),
        })
        private Object value;

        @JsonPropertyDescription("A format string to use as the value of the new entry, for example, " +
                "<code>${key1}-${key2}</code>, where <code>key1</code> and <code>key2</code> are existing keys in the event. Required if neither " +
                "<code>value</code> nor <code>value_expression</code> is specified.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name="value", allowedValues = {"null"}),
                @AlsoRequired.Required(name=VALUE_EXPRESSION_KEY, allowedValues = {"null"})
        })
        @ExampleValues({
                @Example(value = "${/key_one}-${/key_two}", description = "Adds a value as a combination of the existing key_one and key_two values to the key or metadata_key"),
        })
        private String format;

        @JsonProperty(VALUE_EXPRESSION_KEY)
        @JsonPropertyDescription("An expression string to use as the value of the new entry. For example, <code>/key</code> " +
                "is an existing key in the event with a type of either a number, a string, or a Boolean. " +
                "Expressions can also contain functions returning number/string/integer. For example, " +
                "<code>length(/key)</code> will return the length of the key in the event when the key is a string. For more " +
                "information about keys, see <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">Expression syntax</a>.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name="value", allowedValues = {"null"}),
                @AlsoRequired.Required(name="format", allowedValues = {"null"})
        })
        @ExampleValues({
                @Example(value = "length(/my_key)", description = "Adds an integer value based on the length of the existing key 'my_key' to the new key or metadata_key"),
                @Example(value = "/my_key", description = "Adds a value based on the existing value of my_key to the new key or metadata_key"),
        })
        private String valueExpression;

        @JsonProperty(OVERWRITE_IF_KEY_EXISTS_KEY)
        @JsonPropertyDescription("When set to <code>true</code>, the existing value is overwritten if <code>key</code> already exists " +
                "in the event. Only one of <code>overwrite_if_key_exists</code> or <code>append_if_key_exists</code> can be <code>true</code>. The default value is <code>false</code>.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name=APPEND_IF_KEY_EXISTS_KEY, allowedValues = {"false"})
        })
        private boolean overwriteIfKeyExists = false;

        @JsonProperty(APPEND_IF_KEY_EXISTS_KEY)
        @JsonPropertyDescription("When set to <code>true</code>, the existing value will be appended if a <code>key</code> already " +
                "exists in the event. An array will be created if the existing value is not an array. Default is <code>false</code>.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name=OVERWRITE_IF_KEY_EXISTS_KEY, allowedValues = {"false"})
        })
        private boolean appendIfKeyExists = false;

        @JsonProperty("add_when")
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
                "such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will be run on the event.")
        @ExampleValues({
                @Example(value = "/some_key == null", description = "Only runs the add_entries processor if the key some_key is null or does not exist.")
        })
        private String addWhen;

        public String getKey() {
            return key;
        }

        public String getMetadataKey() {
            return metadataKey;
        }

        public Object getValue() {
            return value;
        }

        public String getFormat() {
            return format;
        }

        public String getValueExpression() {
            return valueExpression;
        }

        public boolean getOverwriteIfKeyExists() {
            return overwriteIfKeyExists;
        }

        public boolean getAppendIfKeyExists() {
            return appendIfKeyExists;
        }

        public String getAddWhen() { return addWhen; }

        @AssertTrue(message = "Either value or format or expression must be specified, and only one of them can be specified")
        public boolean hasValueOrFormatOrExpression() {
            return Stream.of(value, format, valueExpression).filter(n -> n!=null).count() == 1;
        }

        @AssertTrue(message = "overwrite_if_key_exists and append_if_key_exists can not be set to true at the same time.")
        boolean overwriteAndAppendNotBothSet() {
            return !(overwriteIfKeyExists && appendIfKeyExists);
        }

        public Entry(final String key,
                     final String metadataKey,
                     final Object value,
                     final String format,
                     final String valueExpression,
                     final boolean overwriteIfKeyExists,
                     final boolean appendIfKeyExists,
                     final String addWhen)
        {
            if (key != null && metadataKey != null) {
                throw new IllegalArgumentException("Only one of the two - key and metadatakey - should be specified");
            }
            if (key == null && metadataKey == null) {
                throw new IllegalArgumentException("At least one of the two - key and metadatakey - must be specified");
            }
            this.key = key;
            this.metadataKey = metadataKey;
            this.value = value;
            this.format = format;
            this.valueExpression = valueExpression;
            this.overwriteIfKeyExists = overwriteIfKeyExists;
            this.appendIfKeyExists = appendIfKeyExists;
            this.addWhen = addWhen;
        }

        public Entry() {

        }
    }

    @NotEmpty
    @NotNull
    @Valid
    @JsonPropertyDescription("A list of entries to add to the event.")
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
