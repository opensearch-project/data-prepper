/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.grok;

import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.List;
import java.util.Map;

public class GrokProcessorConfig {
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

    static final boolean DEFAULT_BREAK_ON_MATCH = true;
    static final boolean DEFAULT_KEEP_EMPTY_CAPTURES = false;
    static final boolean DEFAULT_NAMED_CAPTURES_ONLY = true;
    static final String DEFAULT_PATTERNS_FILES_GLOB = "*";
    static final int DEFAULT_TIMEOUT_MILLIS = 30000;
    static final String DEFAULT_TARGET_KEY = null;

    private final boolean breakOnMatch;
    private final boolean keepEmptyCaptures;
    private final Map<String, List<String>> match;
    private final boolean namedCapturesOnly;
    private final List<String> keysToOverwrite;
    private final List<String> patternsDirectories;
    private final String patternsFilesGlob;
    private final Map<String, String> patternDefinitions;
    private final int timeoutMillis;
    private final String targetKey;
    private final String grokWhen;

    private GrokProcessorConfig(final boolean breakOnMatch,
                                final boolean keepEmptyCaptures,
                                final Map<String, List<String>> match,
                                final boolean namedCapturesOnly,
                                final List<String> keysToOverwrite,
                                final List<String> patternsDirectories,
                                final String patternsFilesGlob,
                                final Map<String, String> patternDefinitions,
                                final int timeoutMillis,
                                final String targetKey,
                                final String grokWhen) {

        this.breakOnMatch = breakOnMatch;
        this.keepEmptyCaptures = keepEmptyCaptures;
        this.match = match;
        this.namedCapturesOnly = namedCapturesOnly;
        this.keysToOverwrite = keysToOverwrite;
        this.patternsDirectories = patternsDirectories;
        this.patternsFilesGlob = patternsFilesGlob;
        this.patternDefinitions = patternDefinitions;
        this.timeoutMillis = timeoutMillis;
        this.targetKey = targetKey;
        this.grokWhen = grokWhen;
    }

    public static GrokProcessorConfig buildConfig(final PluginSetting pluginSetting) {
        return new GrokProcessorConfig(pluginSetting.getBooleanOrDefault(BREAK_ON_MATCH, DEFAULT_BREAK_ON_MATCH),
                pluginSetting.getBooleanOrDefault(KEEP_EMPTY_CAPTURES, DEFAULT_KEEP_EMPTY_CAPTURES),
                pluginSetting.getTypedListMap(MATCH, String.class, String.class),
                pluginSetting.getBooleanOrDefault(NAMED_CAPTURES_ONLY, DEFAULT_NAMED_CAPTURES_ONLY),
                pluginSetting.getTypedList(KEYS_TO_OVERWRITE, String.class),
                pluginSetting.getTypedList(PATTERNS_DIRECTORIES, String.class),
                pluginSetting.getStringOrDefault(PATTERNS_FILES_GLOB, DEFAULT_PATTERNS_FILES_GLOB),
                pluginSetting.getTypedMap(PATTERN_DEFINITIONS, String.class, String.class),
                pluginSetting.getIntegerOrDefault(TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS),
                pluginSetting.getStringOrDefault(TARGET_KEY, DEFAULT_TARGET_KEY),
                pluginSetting.getStringOrDefault(GROK_WHEN, null));
    }

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
}
