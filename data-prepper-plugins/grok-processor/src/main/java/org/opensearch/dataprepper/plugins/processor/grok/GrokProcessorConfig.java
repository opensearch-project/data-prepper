/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.grok;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder
@JsonClassDescription("The <code>grok</code> processor uses pattern matching to structure and extract important keys from " +
        "unstructured data.")
public class GrokProcessorConfig {

    static final String TOTAL_PATTERNS_ATTEMPTED_METADATA_KEY = "_total_grok_patterns_attempted";

    static final String TOTAL_TIME_SPENT_IN_GROK_METADATA_KEY = "_total_grok_processing_time";

    static final String BREAK_ON_MATCH = "break_on_match";
    static final String KEEP_EMPTY_CAPTURES = "keep_empty_captures";
    static final String MATCH = "match";
    static final String NAMED_CAPTURES_ONLY = "named_captures_only";
    static final String KEYS_TO_OVERWRITE= "keys_to_overwrite";
    static final String PATTERN_DEFINITIONS = "pattern_definitions";
    static final String PATTERNS_DIRECTORIES = "patterns_directories";
    static final String PATTERNS_FILES_GLOB = "patterns_files_glob";
    static final String TIMEOUT_MILLIS = "timeout_millis";
    static final String TARGET_KEY = "target_key";
    static final String GROK_WHEN = "grok_when";
    static final String TAGS_ON_MATCH_FAILURE = "tags_on_match_failure";
    static final String TAGS_ON_TIMEOUT = "tags_on_timeout";

    static final String INCLUDE_PERFORMANCE_METADATA = "performance_metadata";

    static final boolean DEFAULT_BREAK_ON_MATCH = true;
    static final boolean DEFAULT_KEEP_EMPTY_CAPTURES = false;
    static final boolean DEFAULT_NAMED_CAPTURES_ONLY = true;
    static final String DEFAULT_PATTERNS_FILES_GLOB = "*";
    static final int DEFAULT_TIMEOUT_MILLIS = 30000;
    static final String DEFAULT_TARGET_KEY = null;

    @JsonProperty(MATCH)
    @NotEmpty
    @NotNull
    @JsonPropertyDescription("Specifies which keys should match specific patterns. " +
            "Each key is a source field. The value is a list of possible grok patterns to match on. " +
            "The <code>grok</code> processor will extract values from the first match for each field. " +
            "Default is an empty response body.")
    private Map<String, List<String>> match = Collections.emptyMap();

    @JsonProperty(TARGET_KEY)
    @JsonPropertyDescription("Specifies a parent-level key used to store all captures. Default value is <code>null</code> which will write captures into the root of the event.")
    private String targetKey = DEFAULT_TARGET_KEY;

    @JsonProperty(value = BREAK_ON_MATCH, defaultValue = "true")
    @JsonPropertyDescription("Specifies whether to match all patterns (<code>false</code>) or stop once the first successful " +
            "match is found (<code>true</code>). Default is <code>true</code>.")
    private boolean breakOnMatch = DEFAULT_BREAK_ON_MATCH;

    @JsonProperty(KEEP_EMPTY_CAPTURES)
    @JsonPropertyDescription("Enables the preservation of <code>null</code> captures from the processed output. Default is <code>false</code>.")
    private boolean keepEmptyCaptures = DEFAULT_KEEP_EMPTY_CAPTURES;

    @JsonProperty(value = NAMED_CAPTURES_ONLY, defaultValue = "true")
    @JsonPropertyDescription("Specifies whether to keep only named captures. Default is <code>true</code>.")
    private boolean namedCapturesOnly = DEFAULT_NAMED_CAPTURES_ONLY;

    @JsonProperty(KEYS_TO_OVERWRITE)
    @JsonPropertyDescription("Specifies which existing keys will be overwritten if there is a capture with the same key value. " +
            "Default is an empty list.")
    private List<String> keysToOverwrite = Collections.emptyList();

    @JsonProperty(PATTERN_DEFINITIONS)
    @JsonPropertyDescription("Allows for a custom pattern that can be used inline inside the response body. " +
            "Default is an empty response body.")
    private Map<String, String> patternDefinitions = Collections.emptyMap();

    @JsonProperty(PATTERNS_DIRECTORIES)
    @JsonPropertyDescription("Specifies which directory paths contain the custom pattern files. Default is an empty list.")
    private List<String> patternsDirectories = Collections.emptyList();

    @JsonProperty(PATTERNS_FILES_GLOB)
    @JsonPropertyDescription("Specifies which pattern files to use from the directories specified for " +
            "<code>pattern_directories</code>. Default is <code>*</code>.")
    private String patternsFilesGlob = DEFAULT_PATTERNS_FILES_GLOB;

    @JsonProperty(TIMEOUT_MILLIS)
    @JsonPropertyDescription("The maximum amount of time during which matching occurs. " +
            "Setting to <code>0</code> prevents any matching from occurring. Default is <code>30000</code>.")
    private int timeoutMillis = DEFAULT_TIMEOUT_MILLIS;

    @JsonProperty(TAGS_ON_MATCH_FAILURE)
    @JsonPropertyDescription("A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when grok fails to " +
            "match or an unknown exception occurs while matching. This tag may be used in conditional expressions in " +
            "other parts of the configuration")
    private List<String> tagsOnMatchFailure = Collections.emptyList();

    @JsonProperty(TAGS_ON_TIMEOUT)
    @JsonPropertyDescription("The tags to add to the event metadata if the grok match times out.")
    private List<String> tagsOnTimeout = Collections.emptyList();

    @JsonProperty(GROK_WHEN)
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>'/test != false'</code>. " +
            "If specified, the <code>grok</code> processor will only run on events when the expression evaluates to true. ")
    private String grokWhen;

    @JsonProperty(INCLUDE_PERFORMANCE_METADATA)
    @JsonPropertyDescription("A boolean value to determine whether to include performance metadata into event metadata. " +
            "If set to true, the events coming out of grok will have new fields such as <code>_total_grok_patterns_attempted</code> and <code>_total_grok_processing_time</code>." +
            "You can use this metadata to perform performance testing and tuning of your grok patterns. By default, it is not included.")
    private boolean includePerformanceMetadata = false;

    public boolean isBreakOnMatch() {
        return breakOnMatch;
    }

    public boolean isKeepEmptyCaptures() {
        return keepEmptyCaptures;
    }

    public Map<String, List<String>> getMatch() {
        return match;
    }

    public boolean isNamedCapturesOnly() {
        return namedCapturesOnly;
    }

    public List<String> getkeysToOverwrite() {
        return keysToOverwrite;
    }

    public List<String> getPatternsDirectories() {
        return patternsDirectories;
    }

    public String getPatternsFilesGlob() {
        return patternsFilesGlob;
    }

    public Map<String, String> getPatternDefinitions() {
        return patternDefinitions;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public String getGrokWhen() { return grokWhen; }

    public List<String> getTagsOnMatchFailure() {
        return tagsOnMatchFailure;
    }

    public List<String> getTagsOnTimeout() {
        return tagsOnTimeout;
    }

    public boolean getIncludePerformanceMetadata() { return includePerformanceMetadata; }
}
