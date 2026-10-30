/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;

import java.util.List;
import org.opensearch.dataprepper.model.pattern.Pattern;
import org.opensearch.dataprepper.model.pattern.PatternSyntaxException;
import java.util.stream.Collectors;

@JsonPropertyOrder
@JsonClassDescription("The <code>select_entries</code> processor selects entries from an event.")
public class SelectEntriesProcessorConfig {

    @JsonProperty("include_keys")
    @JsonPropertyDescription("A list of keys to be selected from an event.")
    private List<String> includeKeys;

    @JsonProperty("include_keys_regex")
    @JsonPropertyDescription("A list of regex patterns to match keys be selected from an event.")
    private List<String> includeKeysRegex;

    @JsonIgnore
    private List<Pattern> includeKeysRegexPatterns;

    // The processor is implemented to support this, but can be made configurable when there is a feature request
    @JsonIgnore
    private String includeKeysRegexPointer;

    @JsonProperty("select_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
            "such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will be " +
            "run on the event. Default is <code>null</code>. All events will be processed unless otherwise stated.")
    @ExampleValues({
        @Example(value = "/some-key == test", description = "Only runs the select_entries processor on the Event if some_key is 'test'.")
    })
    private String selectWhen;

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    public List<Pattern> getIncludeKeysRegex() {
        return includeKeysRegexPatterns;
    }

    public String getIncludeKeysRegexPointer() {
        return includeKeysRegexPointer;
    }

    public String getSelectWhen() {
        return selectWhen;
    }

    private void setIncludeKeysRegex() {
        includeKeysRegexPatterns = includeKeysRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    }


    @AssertTrue(message = "At least one of include_keys and include_keys_regex is required.")
    boolean isValidIncludeKeys() {
        return (includeKeys != null && !includeKeys.isEmpty()) || (includeKeysRegex != null && !includeKeysRegex.isEmpty());
    }

    @AssertTrue(message = "Invalid regex pattern found in include_keys_regex.")
    boolean isValidIncludeKeysRegex() {
        if (includeKeysRegex != null && !includeKeysRegex.isEmpty()) {
            try {
                setIncludeKeysRegex();
            } catch (final PatternSyntaxException e) {
                return false;
            }
        }

        return true;
    }
}

