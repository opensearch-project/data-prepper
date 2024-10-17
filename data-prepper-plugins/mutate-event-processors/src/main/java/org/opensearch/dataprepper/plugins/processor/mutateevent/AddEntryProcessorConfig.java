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

import java.util.List;
import java.util.stream.Stream;

@JsonPropertyOrder
@JsonClassDescription("The <code>add_entries</code> processor adds entries to an event.")
public class AddEntryProcessorConfig {
    @JsonPropertyOrder
    public static class Entry {
        @JsonPropertyDescription("The key of the new entry to be added. Some examples of keys include <code>my_key</code>, " +
                "<code>myKey</code>, and <code>object/sub_Key</code>. The key can also be a format expression, for example, <code>${/key1}</code> to " +
                "use the value of field <code>key1</code> as the key.")
        private String key;

        @JsonProperty("metadata_key")
        @JsonPropertyDescription("The key for the new metadata attribute. The argument must be a literal string key " +
                "and not a JSON Pointer. Either one of <code>key</code> or <code>metadata_key</code> is required.")
        private String metadataKey;

        @JsonPropertyDescription("The value of the new entry to be added, which can be used with any of the " +
                "following data types: strings, Booleans, numbers, null, nested objects, and arrays.")
        private Object value;

        @JsonPropertyDescription("A format string to use as the value of the new entry, for example, " +
                "<code>${key1}-${key2}</code>, where <code>key1</code> and <code>key2</code> are existing keys in the event. Required if neither" +
                "<code>value</code> nor <code>value_expression</code> is specified.")
        private String format;

        @JsonProperty("value_expression")
        @JsonPropertyDescription("An expression string to use as the value of the new entry. For example, <code>/key</code> " +
                "is an existing key in the event with a type of either a number, a string, or a Boolean. " +
                "Expressions can also contain functions returning number/string/integer. For example, " +
                "<code>length(/key)</code> will return the length of the key in the event when the key is a string. For more " +
                "information about keys, see <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">Expression syntax</a>.")
        private String valueExpression;

        @JsonProperty("overwrite_if_key_exists")
        @JsonPropertyDescription("When set to <code>true</code>, the existing value is overwritten if <code>key</code> already exists " +
                "in the event. The default value is <code>false</code>.")
        private boolean overwriteIfKeyExists = false;

        @JsonProperty("append_if_key_exists")
        @JsonPropertyDescription("When set to <code>true</code>, the existing value will be appended if a <code>key</code> already " +
                "exists in the event. An array will be created if the existing value is not an array. Default is <code>false</code>.")
        private boolean appendIfKeyExists = false;

        @JsonProperty("add_when")
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
                "such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will be run on the event.")
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
