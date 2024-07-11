/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

public class SplitStringProcessorConfig implements StringProcessorConfig<SplitStringProcessorConfig.Entry> {
    public static class Entry {

        @NotEmpty
        @NotNull
        @JsonPropertyDescription("The key to split.")
        private EventKey source;

        @JsonProperty("delimiter_regex")
        @JsonPropertyDescription("The regex string responsible for the split. Cannot be defined at the same time as `delimiter`. " +
                "At least `delimiter` or `delimiter_regex` must be defined.")
        private String delimiterRegex;

        @Size(min = 1, max = 1)
        @JsonPropertyDescription("The separator character responsible for the split. " +
                "Cannot be defined at the same time as `delimiter_regex`. " +
                "At least `delimiter` or `delimiter_regex` must be defined.")
        private String delimiter;

        @JsonProperty("split_when")
        @JsonPropertyDescription("Specifies under what condition the `split_string` processor should perform splitting. " +
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

    @JsonPropertyDescription("List of entries. Valid values are `source`, `delimiter`, and `delimiter_regex`.")
    private List<@Valid Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
