/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `replace_string` processor replaces all occurrence of substring in key’s value with a " +
        "replacement string.")
public class ReplaceStringProcessorConfig implements StringProcessorConfig<ReplaceStringProcessorConfig.Entry> {
    public static class Entry {
        @JsonPropertyDescription("The key to modify.")
        private EventKey source;
        @JsonPropertyDescription("The substring to be replaced in the source.")
        private String from;
        @JsonPropertyDescription("The string to be replaced for each match of `from`.")
        private String to;

        @JsonProperty("replace_when")
        @JsonPropertyDescription("A Data Prepper [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), " +
                "such as `/some-key == \"test\"'`, that will be evaluated to determine whether the processor will be " +
                "run on the event. Default is `null`. All events will be processed unless otherwise stated.")
        private String replaceWhen;

        public EventKey getSource() {
            return source;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        public String getReplaceWhen() { return replaceWhen; }

        public Entry(final EventKey source, final String from, final String to, final String replaceWhen) {
            this.source = source;
            this.from = from;
            this.to = to;
            this.replaceWhen = replaceWhen;
        }

        public Entry() {}
    }

    @JsonPropertyDescription("List of entries. Valid values are `source`, `from`, and `to`, and `substitute_when`.")
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public List<Entry> getIterativeConfig() {
        return entries;
    }
}
