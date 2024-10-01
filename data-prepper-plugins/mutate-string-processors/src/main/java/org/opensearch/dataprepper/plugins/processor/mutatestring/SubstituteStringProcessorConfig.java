/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `substitute_string` processor matches a key’s value against a regular expression and " +
        "replaces all matches with a replacement string.")
public class SubstituteStringProcessorConfig implements StringProcessorConfig<SubstituteStringProcessorConfig.Entry> {
    public static class Entry {
        @JsonPropertyDescription("The key to modify.")
        private EventKey source;
        @JsonPropertyDescription("The Regex String to be replaced. Special regex characters such as `[` and `]` must " +
                "be escaped using `\\\\` when using double quotes and `\\ ` when using single quotes. " +
                "See [Java Patterns](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html) " +
                "for more information.")
        private String from;
        @JsonPropertyDescription("The String to be substituted for each match of `from`.")
        private String to;

        @JsonProperty("substitute_when")
        @JsonPropertyDescription("A Data Prepper [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), " +
                "such as `/some-key == \"test\"'`, that will be evaluated to determine whether the processor will be " +
                "run on the event. Default is `null`. All events will be processed unless otherwise stated.")
        private String substituteWhen;

        public EventKey getSource() {
            return source;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        public String getSubstituteWhen() { return substituteWhen; }

        public Entry(final EventKey source, final String from, final String to, final String substituteWhen) {
            this.source = source;
            this.from = from;
            this.to = to;
            this.substituteWhen = substituteWhen;
        }

        public Entry() {}
    }

    @JsonPropertyDescription("List of entries. Valid values are `source`, `from`, and `to`, and `substitute_when`.")
    @NotNull
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public List<Entry> getIterativeConfig() {
        return entries;
    }
}
