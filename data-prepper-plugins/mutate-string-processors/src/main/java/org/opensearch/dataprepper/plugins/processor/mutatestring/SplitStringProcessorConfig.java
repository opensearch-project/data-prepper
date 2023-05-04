/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

public class SplitStringProcessorConfig implements StringProcessorConfig<SplitStringProcessorConfig.Entry> {
    public static class Entry {

        @NotEmpty
        @NotNull
        private String source;

        @JsonProperty("delimiter_regex")
        private String delimiterRegex;

        @Size(min = 1, max = 1)
        private String delimiter;

        @JsonProperty("split_when")
        private String splitWhen;

        public String getSource() {
            return source;
        }

        public String getDelimiterRegex() {
            return delimiterRegex;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public String getSplitWhen() { return splitWhen; }

        public Entry(final String source, final String delimiterRegex, final String delimiter, final String splitWhen) {
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

    private List<@Valid Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
