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
import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperConfig.DEFAULT_BREAK_ON_MATCH;
import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperConfig.DEFAULT_KEEP_EMPTY_CAPTURES;
import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperConfig.DEFAULT_NAMED_CAPTURES_ONLY;
import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperConfig.DEFAULT_PATTERNS_FILES_GLOB;
import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperConfig.DEFAULT_TARGET_KEY;
import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperConfig.DEFAULT_TIMEOUT_MILLIS;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class GrokPrepperConfigTests {
    private static final String PLUGIN_NAME = "grok";

    private static final Map<String, List<String>> TEST_MATCH = new HashMap<>();
    private static final List<String> TEST_KEYS_TO_OVERWRITE = new ArrayList<>();
    private static final List<String> TEST_PATTERNS_DIRECTORIES = new ArrayList<>();
    private static final String TEST_PATTERNS_FILES_GLOB = ".*pat";
    private static final Map<String, String> TEST_PATTERN_DEFINITIONS = new HashMap<>();
    private static final int TEST_TIMEOUT_MILLIS = 100000;
    private static final String TEST_TARGET_KEY = "test_target";

    private static final Map<String, String> TEST_INVALID_MATCH = new HashMap<>();

    @BeforeAll
    public static void setUp() {
        final List<String> log_patterns = new ArrayList<>();
        log_patterns.add("%{SYNTAX:SEMANTIC}");
        log_patterns.add("%{SYNTAX_2:SEMANTIC_2}");

        TEST_MATCH.put("message", Collections.singletonList("%{COMBINEDAPACHELOG}"));
        TEST_MATCH.put("log",log_patterns);

        TEST_INVALID_MATCH.put("invalid_message_key", "invalid_message_value");

        TEST_KEYS_TO_OVERWRITE.add("message");
        TEST_KEYS_TO_OVERWRITE.add("randomKey");

        TEST_PATTERNS_DIRECTORIES.add("/patterns");
        TEST_PATTERNS_DIRECTORIES.add("patterns/second");

        TEST_PATTERN_DEFINITIONS.put("JAVAFILE", "(?:[A-Za-z0-9_.-]+java)");
        TEST_PATTERN_DEFINITIONS.put("BEAGLE", "beagle");
    }

    @Test
    public void testDefault() {
        final GrokPrepperConfig grokPrepperConfig = GrokPrepperConfig.buildConfig(new PluginSetting(PLUGIN_NAME, null));

        assertThat(grokPrepperConfig.isBreakOnMatch(), equalTo(DEFAULT_BREAK_ON_MATCH));
        assertThat(grokPrepperConfig.isKeepEmptyCaptures(), equalTo(DEFAULT_KEEP_EMPTY_CAPTURES));
        assertThat(grokPrepperConfig.getMatch(), equalTo(Collections.emptyMap()));
        assertThat(grokPrepperConfig.getkeysToOverwrite(), equalTo(Collections.emptyList()));
        assertThat(grokPrepperConfig.getPatternDefinitions(), equalTo(Collections.emptyMap()));
        assertThat(grokPrepperConfig.getPatternsDirectories(), equalTo(Collections.emptyList()));
        assertThat(grokPrepperConfig.getPatternsFilesGlob(), equalTo(DEFAULT_PATTERNS_FILES_GLOB));
        assertThat(grokPrepperConfig.getTargetKey(), equalTo(DEFAULT_TARGET_KEY));
        assertThat(grokPrepperConfig.isNamedCapturesOnly(), equalTo(DEFAULT_NAMED_CAPTURES_ONLY));
        assertThat(grokPrepperConfig.getTimeoutMillis(), equalTo(DEFAULT_TIMEOUT_MILLIS));
    }

    @Test
    public void testValidConfig() {
        final PluginSetting validPluginSetting = completePluginSettingForGrokPrepper(
                false,
                true,
                TEST_MATCH,
                false,
                TEST_KEYS_TO_OVERWRITE,
                TEST_PATTERNS_DIRECTORIES,
                TEST_PATTERNS_FILES_GLOB,
                TEST_PATTERN_DEFINITIONS,
                TEST_TIMEOUT_MILLIS,
                TEST_TARGET_KEY);

        final GrokPrepperConfig grokPrepperConfig = GrokPrepperConfig.buildConfig(validPluginSetting);

        assertThat(grokPrepperConfig.isBreakOnMatch(), equalTo(false));
        assertThat(grokPrepperConfig.isKeepEmptyCaptures(), equalTo(true));
        assertThat(grokPrepperConfig.getMatch(), equalTo(TEST_MATCH));
        assertThat(grokPrepperConfig.getkeysToOverwrite(), equalTo(TEST_KEYS_TO_OVERWRITE));
        assertThat(grokPrepperConfig.getPatternDefinitions(), equalTo(TEST_PATTERN_DEFINITIONS));
        assertThat(grokPrepperConfig.getPatternsDirectories(), equalTo(TEST_PATTERNS_DIRECTORIES));
        assertThat(grokPrepperConfig.getPatternsFilesGlob(), equalTo(TEST_PATTERNS_FILES_GLOB));
        assertThat(grokPrepperConfig.getTargetKey(), equalTo(TEST_TARGET_KEY));
        assertThat(grokPrepperConfig.isNamedCapturesOnly(), equalTo(false));
        assertThat(grokPrepperConfig.getTimeoutMillis(), equalTo(TEST_TIMEOUT_MILLIS));
    }

    @Test
    public void testInvalidConfig() {
        final PluginSetting invalidPluginSetting = completePluginSettingForGrokPrepper(
                false,
                true,
                TEST_MATCH,
                false,
                TEST_KEYS_TO_OVERWRITE,
                TEST_PATTERNS_DIRECTORIES,
                TEST_PATTERNS_FILES_GLOB,
                TEST_PATTERN_DEFINITIONS,
                TEST_TIMEOUT_MILLIS,
                TEST_TARGET_KEY);

        invalidPluginSetting.getSettings().put(GrokPrepperConfig.MATCH, TEST_INVALID_MATCH);

        assertThrows(IllegalArgumentException.class, () -> GrokPrepperConfig.buildConfig(invalidPluginSetting));
    }

    private PluginSetting completePluginSettingForGrokPrepper(final boolean breakOnMatch,
                                                              final boolean keepEmptyCaptures,
                                                              final Map<String, List<String>> match,
                                                              final boolean namedCapturesOnly,
                                                              final List<String> keysToOverwrite,
                                                              final List<String> patternsDirectories,
                                                              final String patternsFilesGlob,
                                                              final Map<String, String> patternDefinitions,
                                                              final int timeoutMillis,
                                                              final String targetKey) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(GrokPrepperConfig.BREAK_ON_MATCH, breakOnMatch);
        settings.put(GrokPrepperConfig.NAMED_CAPTURES_ONLY, namedCapturesOnly);
        settings.put(GrokPrepperConfig.MATCH, match);
        settings.put(GrokPrepperConfig.KEEP_EMPTY_CAPTURES, keepEmptyCaptures);
        settings.put(GrokPrepperConfig.KEYS_TO_OVERWRITE, keysToOverwrite);
        settings.put(GrokPrepperConfig.PATTERNS_DIRECTORIES, patternsDirectories);
        settings.put(GrokPrepperConfig.PATTERN_DEFINITIONS, patternDefinitions);
        settings.put(GrokPrepperConfig.PATTERNS_FILES_GLOB, patternsFilesGlob);
        settings.put(GrokPrepperConfig.TIMEOUT_MILLIS, timeoutMillis);
        settings.put(GrokPrepperConfig.TARGET_KEY, targetKey);

        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
