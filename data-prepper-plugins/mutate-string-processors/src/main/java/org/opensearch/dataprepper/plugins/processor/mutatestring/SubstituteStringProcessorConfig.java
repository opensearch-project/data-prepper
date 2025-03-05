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
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.model.annotations.ValidRegex;
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

        @NotNull
        @ValidRegex(message = "The value of from is not a valid regex string")
        @JsonPropertyDescription("The regular expression to match on for replacement. Special regex characters such as <code>[</code> and <code>]</code> must " +
                "be escaped using <code>\\\\</code> when using double quotes and <code>\\</code> when using single quotes. " +
                "See <a href=\"https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Pattern.html\">Java Patterns</a> " +
                "for more information.")
        @ExampleValues({
                @Example(value = "/+", description = "Matches at least one forward slash and can be used to normalize a path."),
                @Example(value = "[?&#=]", description = "Matches some special characters common in URIs.")
        })
        private String from;

        @JsonPropertyDescription("The string to be substituted for each match of <code>from</code>.")
        private String to;

        @JsonProperty("substitute_when")
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> " +
                "that will be evaluated to determine whether the processor will be run on the event. " +
                "By default, all events will be processed if no condition is provided.")
        @ExampleValues(
                @Example(value = "startsWith(/request_uri, \"https://\")", description = "Substitute on the string only if it starts with an HTTPS scheme.")
        )
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
