/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class SplitStringProcessorConfig {
    public static class Entry {

        @NotEmpty
        @NotNull
        private String source;

        @NotEmpty
        @NotNull
        private String delimiter;

        public String getSource() {
            return source;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public Entry(final String source, final String delimiter) {
            this.source = source;
            this.delimiter = delimiter;
        }

        public Entry() {};

        private List<Entry> entries;

        public List<Entry> getEntries() {
            return entries;
        }
    }
}
