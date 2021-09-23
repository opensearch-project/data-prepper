/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.grok;

import com.amazon.dataprepper.model.configuration.PluginSetting;

import java.util.List;
import java.util.Map;

public class GrokPrepperConfig {
    static final String BREAK_ON_MATCH = "break_on_match";
    static final String KEEP_EMPTY_CAPTURES = "keep_empty_captures";
    static final String MATCH = "match";
    static final String NAMED_CAPTURES_ONLY = "named_captures_only";
    static final String OVERWRITE= "overwrite";
    static final String PATTERN_DEFINITIONS = "pattern_definitions";
    static final String PATTERNS_DIR = "patterns_dir";
    static final String PATTERNS_FILES_GLOB = "patterns_files_glob";
    static final String TIMEOUT_MILLIS = "timeout_millis";
    static final String TARGET = "target";

    static final boolean DEFAULT_BREAK_ON_MATCH = true;
    static final boolean DEFAULT_KEEP_EMPTY_CAPTURES = false;
    static final boolean DEFAULT_NAMED_CAPTURES_ONLY = true;
    static final String DEFAULT_PATTERNS_FILES_GLOB = ".*";
    static final int DEFAULT_TIMEOUT_MILLIS = 30000;
    static final String DEFAULT_TARGET = null;

    private final boolean breakOnMatch;
    private final boolean keepEmptyCaptures;
    private final Map<String, List<String>> match;
    private final boolean namedCapturesOnly;
    private final List<String> overwrite;
    private final List<String> patternsDir;
    private final String patternsFilesGlob;
    private final Map<String, String> patternDefinitions;
    private final int timeoutMillis;
    private final String target;

    private GrokPrepperConfig(final boolean breakOnMatch,
                              final boolean keepEmptyCaptures,
                              final Map<String, List<String>> match,
                              final boolean namedCapturesOnly,
                              final List<String> overwrite,
                              final List<String> patternsDir,
                              final String patternsFilesGlob,
                              final Map<String, String> patternDefinitions,
                              final int timeoutMillis,
                              final String target) {

        this.breakOnMatch = breakOnMatch;
        this.keepEmptyCaptures = keepEmptyCaptures;
        this.match = match;
        this.namedCapturesOnly = namedCapturesOnly;
        this.overwrite = overwrite;
        this.patternsDir = patternsDir;
        this.patternsFilesGlob = patternsFilesGlob;
        this.patternDefinitions = patternDefinitions;
        this.timeoutMillis = timeoutMillis;
        this.target = target;
    }

    public static GrokPrepperConfig buildConfig(final PluginSetting pluginSetting) {
        return new GrokPrepperConfig(pluginSetting.getBooleanOrDefault(BREAK_ON_MATCH, DEFAULT_BREAK_ON_MATCH),
                pluginSetting.getBooleanOrDefault(KEEP_EMPTY_CAPTURES, DEFAULT_KEEP_EMPTY_CAPTURES),
                pluginSetting.getTypedListMap(MATCH, String.class, String.class),
                pluginSetting.getBooleanOrDefault(NAMED_CAPTURES_ONLY, DEFAULT_NAMED_CAPTURES_ONLY),
                pluginSetting.getTypedList(OVERWRITE, String.class),
                pluginSetting.getTypedList(PATTERNS_DIR, String.class),
                pluginSetting.getStringOrDefault(PATTERNS_FILES_GLOB, DEFAULT_PATTERNS_FILES_GLOB),
                pluginSetting.getTypedMap(PATTERN_DEFINITIONS, String.class, String.class),
                pluginSetting.getIntegerOrDefault(TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS),
                pluginSetting.getStringOrDefault(TARGET, DEFAULT_TARGET));
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

    public List<String> getOverwrite() {
        return overwrite;
    }

    public List<String> getPatternsDir() {
        return patternsDir;
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

    public String getTarget() {
        return target;
    }
}