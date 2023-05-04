/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.grok;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
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
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorConfig.DEFAULT_BREAK_ON_MATCH;
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorConfig.DEFAULT_KEEP_EMPTY_CAPTURES;
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorConfig.DEFAULT_NAMED_CAPTURES_ONLY;
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorConfig.DEFAULT_PATTERNS_FILES_GLOB;
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorConfig.DEFAULT_TARGET_KEY;
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorConfig.DEFAULT_TIMEOUT_MILLIS;

public class GrokProcessorConfigTests {
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
        final GrokProcessorConfig grokProcessorConfig = GrokProcessorConfig.buildConfig(new PluginSetting(PLUGIN_NAME, null));

        assertThat(grokProcessorConfig.isBreakOnMatch(), equalTo(DEFAULT_BREAK_ON_MATCH));
        assertThat(grokProcessorConfig.isKeepEmptyCaptures(), equalTo(DEFAULT_KEEP_EMPTY_CAPTURES));
        assertThat(grokProcessorConfig.getMatch(), equalTo(Collections.emptyMap()));
        assertThat(grokProcessorConfig.getkeysToOverwrite(), equalTo(Collections.emptyList()));
        assertThat(grokProcessorConfig.getPatternDefinitions(), equalTo(Collections.emptyMap()));
        assertThat(grokProcessorConfig.getPatternsDirectories(), equalTo(Collections.emptyList()));
        assertThat(grokProcessorConfig.getPatternsFilesGlob(), equalTo(DEFAULT_PATTERNS_FILES_GLOB));
        assertThat(grokProcessorConfig.getTargetKey(), equalTo(DEFAULT_TARGET_KEY));
        assertThat(grokProcessorConfig.isNamedCapturesOnly(), equalTo(DEFAULT_NAMED_CAPTURES_ONLY));
        assertThat(grokProcessorConfig.getTimeoutMillis(), equalTo(DEFAULT_TIMEOUT_MILLIS));
        assertThat(grokProcessorConfig.getGrokWhen(), equalTo(null));
    }

    @Test
    public void testValidConfig() {
        final PluginSetting validPluginSetting = completePluginSettingForGrokProcessor(
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

        final GrokProcessorConfig grokProcessorConfig = GrokProcessorConfig.buildConfig(validPluginSetting);

        assertThat(grokProcessorConfig.isBreakOnMatch(), equalTo(false));
        assertThat(grokProcessorConfig.isKeepEmptyCaptures(), equalTo(true));
        assertThat(grokProcessorConfig.getMatch(), equalTo(TEST_MATCH));
        assertThat(grokProcessorConfig.getkeysToOverwrite(), equalTo(TEST_KEYS_TO_OVERWRITE));
        assertThat(grokProcessorConfig.getPatternDefinitions(), equalTo(TEST_PATTERN_DEFINITIONS));
        assertThat(grokProcessorConfig.getPatternsDirectories(), equalTo(TEST_PATTERNS_DIRECTORIES));
        assertThat(grokProcessorConfig.getPatternsFilesGlob(), equalTo(TEST_PATTERNS_FILES_GLOB));
        assertThat(grokProcessorConfig.getTargetKey(), equalTo(TEST_TARGET_KEY));
        assertThat(grokProcessorConfig.isNamedCapturesOnly(), equalTo(false));
        assertThat(grokProcessorConfig.getTimeoutMillis(), equalTo(TEST_TIMEOUT_MILLIS));
    }

    @Test
    public void testInvalidConfig() {
        final PluginSetting invalidPluginSetting = completePluginSettingForGrokProcessor(
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

        invalidPluginSetting.getSettings().put(GrokProcessorConfig.MATCH, TEST_INVALID_MATCH);

        assertThrows(IllegalArgumentException.class, () -> GrokProcessorConfig.buildConfig(invalidPluginSetting));
    }

    private PluginSetting completePluginSettingForGrokProcessor(final boolean breakOnMatch,
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
        settings.put(GrokProcessorConfig.BREAK_ON_MATCH, breakOnMatch);
        settings.put(GrokProcessorConfig.NAMED_CAPTURES_ONLY, namedCapturesOnly);
        settings.put(GrokProcessorConfig.MATCH, match);
        settings.put(GrokProcessorConfig.KEEP_EMPTY_CAPTURES, keepEmptyCaptures);
        settings.put(GrokProcessorConfig.KEYS_TO_OVERWRITE, keysToOverwrite);
        settings.put(GrokProcessorConfig.PATTERNS_DIRECTORIES, patternsDirectories);
        settings.put(GrokProcessorConfig.PATTERN_DEFINITIONS, patternDefinitions);
        settings.put(GrokProcessorConfig.PATTERNS_FILES_GLOB, patternsFilesGlob);
        settings.put(GrokProcessorConfig.TIMEOUT_MILLIS, timeoutMillis);
        settings.put(GrokProcessorConfig.TARGET_KEY, targetKey);

        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
