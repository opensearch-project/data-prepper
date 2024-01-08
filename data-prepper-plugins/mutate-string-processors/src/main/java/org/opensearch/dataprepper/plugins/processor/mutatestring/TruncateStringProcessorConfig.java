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

import java.util.List;

public class TruncateStringProcessorConfig implements StringProcessorConfig<TruncateStringProcessorConfig.Entry> {
    public static class Entry {

        @NotEmpty
        @NotNull
        private String source;

        @JsonProperty("length")
        @NotNull
        private int length;

        @JsonProperty("truncate_when")
        private String truncateWhen;

        public String getSource() {
            return source;
        }

        public int getLength() {
            return length;
        }

        public String getTruncateWhen() { return truncateWhen; }

        public Entry(final String source, final int length, final String truncateWhen) {
            this.source = source;
            this.length = length;
            this.truncateWhen = truncateWhen;
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

