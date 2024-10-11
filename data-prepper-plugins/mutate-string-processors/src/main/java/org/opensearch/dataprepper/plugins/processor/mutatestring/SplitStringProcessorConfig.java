/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>split_string</code> processor splits a field into an array using a delimiting character.")
public class SplitStringProcessorConfig implements StringProcessorConfig<SplitStringProcessorConfig.Entry> {
    @JsonPropertyOrder
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonPropertyDescription("The key name of the field to split.")
        private EventKey source;

        @Size(min = 1, max = 1)
        @JsonPropertyDescription("The separator character responsible for the split. " +
                "Cannot be defined at the same time as <code>delimiter_regex</code>. " +
                "At least <code>delimiter</code> or <code>delimiter_regex</code> must be defined.")
        private String delimiter;

        @JsonProperty("delimiter_regex")
        @JsonPropertyDescription("The regex string responsible for the split. Cannot be defined at the same time as <code>delimiter</code>. " +
                "At least <code>delimiter</code> or <code>delimiter_regex</code> must be defined.")
        private String delimiterRegex;

        @JsonProperty("split_when")
        @JsonPropertyDescription("Specifies under what condition the <code>split_string</code> processor should perform splitting. " +
                "Default is no condition.")
        private String splitWhen;

        public EventKey getSource() {
            return source;
        }

        public String getDelimiterRegex() {
            return delimiterRegex;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public String getSplitWhen() { return splitWhen; }

        public Entry(final EventKey source, final String delimiterRegex, final String delimiter, final String splitWhen) {
            this.source = source;
            this.delimiterRegex = delimiterRegex;
            this.delimiter = delimiter;
            this.splitWhen = splitWhen;
        }

        public Entry() {}
    }

    @Override
    @JsonIgnore
    public List<Entry> getIterativeConfig() {
        return entries;
    }

    @JsonPropertyDescription("List of entries. Each entry defines a split.")
    @NotNull
    private List<@Valid Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
