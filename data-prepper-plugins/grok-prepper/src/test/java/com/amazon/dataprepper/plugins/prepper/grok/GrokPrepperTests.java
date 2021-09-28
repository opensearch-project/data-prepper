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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.Mock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class GrokPrepperTests {

    private PluginSetting pluginSetting;
    private GrokPrepper grokPrepper;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    @Mock
    private GrokCompiler grokCompiler;

    @Mock
    private Match match;

    @Mock
    private Grok grok;

    private final String PLUGIN_NAME = "grok";
    private Map<String, Object> capture;

    @BeforeEach
    public void setup() {

        try (MockedStatic<GrokCompiler> grokCompilerMockedStatic = mockStatic(GrokCompiler.class)) {

            capture = new HashMap<>();
            capture.put("field_capture_1", "value_capture_1");
            capture.put("field_capture_2", "value_capture_2");
            capture.put("field_capture_3", "value_capture_3");

            final Map<String, List<String>> matchConfig = new HashMap<>();
            matchConfig.put("message", new ArrayList<String>() {{
                add("%{COMMONAPACHELOG");
            }});

            grokCompilerMockedStatic.when(GrokCompiler::newInstance).thenReturn(grokCompiler);
            when(grokCompiler.compile(anyString(), anyBoolean())).thenReturn(grok);
            when(grok.match(anyString())).thenReturn(match);
            when(match.capture()).thenReturn(capture);

            pluginSetting = completePluginSettingForGrokPrepper(
                    GrokPrepperConfig.DEFAULT_BREAK_ON_MATCH,
                    GrokPrepperConfig.DEFAULT_KEEP_EMPTY_CAPTURES,
                    matchConfig,
                    GrokPrepperConfig.DEFAULT_NAMED_CAPTURES_ONLY,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    GrokPrepperConfig.DEFAULT_PATTERNS_FILES_GLOB,
                    Collections.emptyMap(),
                    GrokPrepperConfig.DEFAULT_TIMEOUT_MILLIS,
                    GrokPrepperConfig.TARGET);

            pluginSetting.setPipelineName("grokPipeline");
            grokPrepper = new GrokPrepper(pluginSetting);
        }
    }

    @AfterEach
    public void tearDown() {
      grokPrepper.shutdown();
    }

    @Test
    public void singleMatchMergeTest() throws JsonProcessingException {

        String testData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\"}";
        Record<String> record = new Record<>(testData);

        String resultData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\","
                                .concat("\"field_capture_1\":\"value_capture_1\",")
                                .concat("\"field_capture_2\":\"value_capture_2\",")
                                .concat("\"field_capture_3\":\"value_capture_3\"}");

        Record<String> resultRecord = new Record<>(resultData);

        List<Record<String>> grokkedRecords = (List<Record<String>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertThat(equalRecords(grokkedRecords.get(0), resultRecord), equalTo(true));
    }

    @Test
    public void singleMatchMergeCollisionStringsTest() throws JsonProcessingException {
        String testData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\","
                .concat("\"field_capture_1\":\"value_capture_collision\"}");

        Record<String> record = new Record<>(testData);

        String resultData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\","
                .concat("\"field_capture_1\":[\"value_capture_collision\",")
                .concat("\"value_capture_1\"],")
                .concat("\"field_capture_2\":\"value_capture_2\",")
                .concat("\"field_capture_3\":\"value_capture_3\"}");

        Record<String> resultRecord = new Record<>(resultData);

        List<Record<String>> grokkedRecords = (List<Record<String>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertThat(equalRecords(grokkedRecords.get(0), resultRecord), equalTo(true));
    }

    @Test
    public void singleMatchMergeCollisionIntsTest() throws JsonProcessingException {
        capture.put("field_capture_1", 20);

        String testData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\","
                .concat("\"field_capture_1\": 10}");

        Record<String> record = new Record<>(testData);

        String resultData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\","
                .concat("\"field_capture_1\":[ 10,")
                .concat("20 ],")
                .concat("\"field_capture_2\":\"value_capture_2\",")
                .concat("\"field_capture_3\":\"value_capture_3\"}");

        Record<String> resultRecord = new Record<>(resultData);

        List<Record<String>> grokkedRecords = (List<Record<String>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertThat(equalRecords(grokkedRecords.get(0), resultRecord), equalTo(true));
    }

    @Test
    public void singleMatchMergeCollisionMixedTypesTest() throws JsonProcessingException {
        capture.put("field_capture_1", "20");

        String testData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\","
                .concat("\"field_capture_1\": 10}");

        Record<String> record = new Record<>(testData);

        String resultData = "{\"message\":\"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\","
                .concat("\"field_capture_1\":[ 10,")
                .concat("\"20\" ],")
                .concat("\"field_capture_2\":\"value_capture_2\",")
                .concat("\"field_capture_3\":\"value_capture_3\"}");

        Record<String> resultRecord = new Record<>(resultData);

        List<Record<String>> grokkedRecords = (List<Record<String>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertThat(equalRecords(grokkedRecords.get(0), resultRecord), equalTo(true));
    }

    private PluginSetting completePluginSettingForGrokPrepper(final boolean breakOnMatch,
                                                              final boolean keepEmptyCaptures,
                                                              final Map<String, List<String>> match,
                                                              final boolean namedCapturesOnly,
                                                              final List<String> overwrite,
                                                              final List<String> patternsDir,
                                                              final String patternsFilesGlob,
                                                              final Map<String, String> patternDefinitions,
                                                              final int timeoutMillis,
                                                              final String target) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(GrokPrepperConfig.BREAK_ON_MATCH, breakOnMatch);
        settings.put(GrokPrepperConfig.NAMED_CAPTURES_ONLY, namedCapturesOnly);
        settings.put(GrokPrepperConfig.MATCH, match);
        settings.put(GrokPrepperConfig.KEEP_EMPTY_CAPTURES, keepEmptyCaptures);
        settings.put(GrokPrepperConfig.OVERWRITE, overwrite);
        settings.put(GrokPrepperConfig.PATTERNS_DIR, patternsDir);
        settings.put(GrokPrepperConfig.PATTERN_DEFINITIONS, patternDefinitions);
        settings.put(GrokPrepperConfig.PATTERNS_FILES_GLOB, patternsFilesGlob);
        settings.put(GrokPrepperConfig.TIMEOUT_MILLIS, timeoutMillis);
        settings.put(GrokPrepperConfig.TARGET, target);

        return new PluginSetting(PLUGIN_NAME, settings);
    }

    private boolean equalRecords(final Record<String> first, final Record<String> second) throws JsonProcessingException {
        final Map<String, Object> recordMapFirst = OBJECT_MAPPER.readValue(first.getData(), MAP_TYPE_REFERENCE);
        final Map<String, Object> recordMapSecond = OBJECT_MAPPER.readValue(second.getData(), MAP_TYPE_REFERENCE);

        return recordMapFirst.equals(recordMapSecond);
    }
}
