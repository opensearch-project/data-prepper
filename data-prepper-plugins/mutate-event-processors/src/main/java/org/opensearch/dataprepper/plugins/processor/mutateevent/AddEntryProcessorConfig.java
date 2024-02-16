/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.stream.Stream;

public class AddEntryProcessorConfig {
    public static class Entry {
        private String key;

        @JsonProperty("metadata_key")
        private String metadataKey;

        private Object value;

        private String format;

        @JsonProperty("value_expression")
        private String valueExpression;

        @JsonProperty("add_when")
        private String addWhen;

        @JsonProperty("overwrite_if_key_exists")
        private boolean overwriteIfKeyExists = false;

        @JsonProperty("append_if_key_exists")
        private boolean appendIfKeyExists = false;

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

        @AssertTrue(message = "overwrite_if_key_exists and append_if_key_exists can not be set at the same time.")
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
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
