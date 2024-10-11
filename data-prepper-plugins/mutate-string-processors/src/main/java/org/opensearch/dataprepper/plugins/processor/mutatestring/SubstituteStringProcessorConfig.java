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
@JsonClassDescription("The <code>substitute_string</code> processor matches a key’s value against a regular expression and " +
        "replaces all matches with a replacement string.")
public class SubstituteStringProcessorConfig implements StringProcessorConfig<SubstituteStringProcessorConfig.Entry> {
    @JsonPropertyOrder
    public static class Entry {
        @JsonPropertyDescription("The key of the field to modify.")
        private EventKey source;

        @JsonPropertyDescription("The regular expression to match on for replacement. Special regex characters such as <code>[</code> and <code>]</code> must " +
                "be escaped using <code>\\\\</code> when using double quotes and <code>\\</code> when using single quotes. " +
                "See <a href=\"https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html\">Java Patterns</a>" +
                "for more information.")
        private String from;

        @JsonPropertyDescription("The string to be substituted for each match of <code>from</code>.")
        private String to;

        @JsonProperty("substitute_when")
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
                "such as <code>/some-key == \"test\"'</code>, that will be evaluated to determine whether the processor will be " +
                "run on the event. By default, all events will be processed unless otherwise stated.")
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

    @JsonPropertyDescription("List of entries. Each entry defines a substitution.")
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
