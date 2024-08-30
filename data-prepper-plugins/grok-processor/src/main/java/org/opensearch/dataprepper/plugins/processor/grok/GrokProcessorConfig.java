/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.grok;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder
@JsonClassDescription("The `grok` processor uses pattern matching to structure and extract important keys from " +
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

    @JsonProperty(BREAK_ON_MATCH)
    @JsonPropertyDescription("Specifies whether to match all patterns (`false`) or stop once the first successful " +
            "match is found (`true`). Default is `true`.")
    private boolean breakOnMatch = DEFAULT_BREAK_ON_MATCH;
    @JsonProperty(KEEP_EMPTY_CAPTURES)
    @JsonPropertyDescription("Enables the preservation of `null` captures from the processed output. Default is `false`.")
    private boolean keepEmptyCaptures = DEFAULT_KEEP_EMPTY_CAPTURES;
    @JsonProperty(MATCH)
    @JsonPropertyDescription("Specifies which keys should match specific patterns. Default is an empty response body.")
    private Map<String, List<String>> match = Collections.emptyMap();
    @JsonProperty(NAMED_CAPTURES_ONLY)
    @JsonPropertyDescription("Specifies whether to keep only named captures. Default is `true`.")
    private boolean namedCapturesOnly = DEFAULT_NAMED_CAPTURES_ONLY;
    @JsonProperty(KEYS_TO_OVERWRITE)
    @JsonPropertyDescription("Specifies which existing keys will be overwritten if there is a capture with the same key value. " +
            "Default is `[]`.")
    private List<String> keysToOverwrite = Collections.emptyList();
    @JsonProperty(PATTERNS_DIRECTORIES)
    @JsonPropertyDescription("Specifies which directory paths contain the custom pattern files. Default is an empty list.")
    private List<String> patternsDirectories = Collections.emptyList();
    @JsonProperty(PATTERNS_FILES_GLOB)
    @JsonPropertyDescription("Specifies which pattern files to use from the directories specified for " +
            "`pattern_directories`. Default is `*`.")
    private String patternsFilesGlob = DEFAULT_PATTERNS_FILES_GLOB;
    @JsonProperty(PATTERN_DEFINITIONS)
    @JsonPropertyDescription("Allows for a custom pattern that can be used inline inside the response body. " +
            "Default is an empty response body.")
    private Map<String, String> patternDefinitions = Collections.emptyMap();
    @JsonProperty(value = TIMEOUT_MILLIS)
    @JsonPropertyDescription("The maximum amount of time during which matching occurs. " +
            "Setting to `0` prevents any matching from occurring. Default is `30,000`.")
    private int timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    @JsonProperty(TARGET_KEY)
    @JsonPropertyDescription("Specifies a parent-level key used to store all captures. Default value is `null`.")
    private String targetKey = DEFAULT_TARGET_KEY;
    @JsonProperty(GROK_WHEN)
    @JsonPropertyDescription("Specifies under what condition the `grok` processor should perform matching. " +
            "Default is no condition.")
    private String grokWhen;
    @JsonProperty(TAGS_ON_MATCH_FAILURE)
    @JsonPropertyDescription("A `List` of `String`s that specifies the tags to be set in the event when grok fails to " +
            "match or an unknown exception occurs while matching. This tag may be used in conditional expressions in " +
            "other parts of the configuration")
    private List<String> tagsOnMatchFailure = Collections.emptyList();
    @JsonProperty(TAGS_ON_TIMEOUT)
    @JsonPropertyDescription("A `List` of `String`s that specifies the tags to be set in the event when grok match times out.")
    private List<String> tagsOnTimeout = Collections.emptyList();
    @JsonProperty(INCLUDE_PERFORMANCE_METADATA)
    @JsonPropertyDescription("A `Boolean` on whether to include performance metadata into event metadata, " +
            "e.g. _total_grok_patterns_attempted, _total_grok_processing_time.")
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
