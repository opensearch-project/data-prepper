/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.grok;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorTests.buildRecordWithEvent;

public class GrokProcessorIT {
    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;
    private GrokProcessorConfig grokProcessorConfig;
    private GrokProcessor grokProcessor;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private final String PLUGIN_NAME = "grok";
    private String messageInput;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    public void setup() {

        pluginSetting = completePluginSettingForGrokProcessor(
                GrokProcessorConfig.DEFAULT_BREAK_ON_MATCH,
                GrokProcessorConfig.DEFAULT_KEEP_EMPTY_CAPTURES,
                Collections.emptyMap(),
                GrokProcessorConfig.DEFAULT_NAMED_CAPTURES_ONLY,
                Collections.emptyList(),
                Collections.emptyList(),
                GrokProcessorConfig.DEFAULT_PATTERNS_FILES_GLOB,
                Collections.emptyMap(),
                GrokProcessorConfig.DEFAULT_TIMEOUT_MILLIS,
                GrokProcessorConfig.DEFAULT_TARGET_KEY,
                null,
                null);

        pluginSetting.setPipelineName("grokPipeline");
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);

        // This is a COMMONAPACHELOG pattern with the following format
        // COMMONAPACHELOG %{IPORHOST:clientip} %{USER:ident} %{USER:auth} \[%{HTTPDATE:timestamp}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})" %{NUMBER:response} (?:%{NUMBER:bytes}|-)
        // Note that rawrequest is missing from the log below, which means that it will not be captured unless keep_empty_captures is true
        messageInput = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";
    }

    @AfterEach
    public void tearDown() {
        grokProcessor.shutdown();
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
                                                              final String targetKey,
                                                              final String grokWhen,
                                                              final List<String> tagsOnMatchFailure
) {
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
        settings.put(GrokProcessorConfig.GROK_WHEN, grokWhen);
        settings.put(GrokProcessorConfig.TAGS_ON_MATCH_FAILURE, tagsOnMatchFailure);

        return new PluginSetting(PLUGIN_NAME, settings);
    }

    @Test
    public void testMatchNoCapturesWithExistingAndNonExistingKey() throws JsonProcessingException {
        final String nonMatchingPattern = "%{SYSLOGBASE}";
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList(nonMatchingPattern));
        matchConfig.put("bad_key", Collections.singletonList(nonMatchingPattern));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);

        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), record);
    }

    @Test
    public void testSingleMatchSinglePatternWithDefaults() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{COMMONAPACHELOG}"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("clientip", "127.0.0.1");
        resultData.put("ident", "user-identifier");
        resultData.put("auth", "frank");
        resultData.put("timestamp", "10/Oct/2000:13:55:36 -0700");
        resultData.put("verb", "GET");
        resultData.put("request", "/apache_pb.gif");
        resultData.put("httpversion", "1.0");
        resultData.put("response", "200");
        resultData.put("bytes", "2326");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testSingleMatchMultiplePatternWithBreakOnMatchFalse() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        final List<String> patternsToMatchMessage = new ArrayList<>();
        patternsToMatchMessage.add("%{COMMONAPACHELOG}");
        patternsToMatchMessage.add("%{IPORHOST:custom_client_field}");

        matchConfig.put("message", patternsToMatchMessage);

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.BREAK_ON_MATCH, false);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("clientip", "127.0.0.1");
        resultData.put("ident", "user-identifier");
        resultData.put("auth", "frank");
        resultData.put("timestamp", "10/Oct/2000:13:55:36 -0700");
        resultData.put("verb", "GET");
        resultData.put("request", "/apache_pb.gif");
        resultData.put("httpversion", "1.0");
        resultData.put("response", "200");
        resultData.put("custom_client_field", "127.0.0.1");
        resultData.put("bytes", "2326");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testSingleMatchTypeConversionWithDefaults() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("\"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\" %{NUMBER:response:int} (?:%{NUMBER:bytes:float}|-)"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("verb", "GET");
        resultData.put("request", "/apache_pb.gif");
        resultData.put("httpversion", "1.0");
        resultData.put("response", 200);
        resultData.put("bytes", 2326.0);

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testMultipleMatchWithBreakOnMatchFalse() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{COMMONAPACHELOG}"));
        matchConfig.put("extra_field", Collections.singletonList("%{GREEDYDATA} %{IPORHOST:host}"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.BREAK_ON_MATCH, false);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        testData.put("extra_field", "My host IP is 192.0.2.1");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("extra_field", "My host IP is 192.0.2.1");
        resultData.put("clientip", "127.0.0.1");
        resultData.put("ident", "user-identifier");
        resultData.put("auth", "frank");
        resultData.put("timestamp", "10/Oct/2000:13:55:36 -0700");
        resultData.put("verb", "GET");
        resultData.put("request", "/apache_pb.gif");
        resultData.put("httpversion", "1.0");
        resultData.put("response", "200");
        resultData.put("bytes", "2326");
        resultData.put("host", "192.0.2.1");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testMatchWithKeepEmptyCapturesTrue() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{COMMONAPACHELOG}"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.KEEP_EMPTY_CAPTURES, true);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("clientip", "127.0.0.1");
        resultData.put("ident", "user-identifier");
        resultData.put("auth", "frank");
        resultData.put("timestamp", "10/Oct/2000:13:55:36 -0700");
        resultData.put("verb", "GET");
        resultData.put("request", "/apache_pb.gif");
        resultData.put("rawrequest", null);
        resultData.put("httpversion", "1.0");
        resultData.put("response", "200");
        resultData.put("bytes", "2326");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testMatchWithNamedCapturesOnlyFalse() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{GREEDYDATA} %{IPORHOST:host} %{NUMBER}"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.NAMED_CAPTURES_ONLY, false);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "This is my greedy data before matching 192.0.2.1 123456");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "This is my greedy data before matching 192.0.2.1 123456");
        resultData.put("NUMBER", "123456");
        resultData.put("GREEDYDATA", "This is my greedy data before matching");
        resultData.put("host", "192.0.2.1");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testPatternDefinitions() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{GREEDYDATA:greedy_data} %{CUSTOMPHONENUMBERPATTERN:my_number}"));

        final Map<String, String> patternDefinitions = new HashMap<>();
        patternDefinitions.put("CUSTOMPHONENUMBERPATTERN", "\\d\\d\\d-\\d\\d\\d-\\d\\d\\d");

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.PATTERN_DEFINITIONS, patternDefinitions);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "This is my greedy data before matching with my phone number 123-456-789");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "This is my greedy data before matching with my phone number 123-456-789");
        resultData.put("greedy_data", "This is my greedy data before matching with my phone number");
        resultData.put("my_number", "123-456-789");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testPatternsDirWithDefaultPatternsFilesGlob() throws JsonProcessingException {
        final String patternDirectory = "./src/test/resources/test_patterns";

        final List<String> patternsDirectories = new ArrayList<>();
        patternsDirectories.add(patternDirectory);

        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("My birthday is %{CUSTOMBIRTHDAYPATTERN:my_birthday} and my phone number is %{CUSTOMPHONENUMBERPATTERN:my_number}"));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "My birthday is April 15, 1991 and my phone number is 123-456-789");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "My birthday is April 15, 1991 and my phone number is 123-456-789");
        resultData.put("my_birthday", "April 15, 1991");
        resultData.put("my_number", "123-456-789");

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.PATTERNS_DIRECTORIES, patternsDirectories);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testPatternsDirWithCustomPatternsFilesGlob() throws JsonProcessingException {
        final String patternDirectory = "./src/test/resources/test_patterns";

        final List<String> patternsDirectories = new ArrayList<>();
        patternsDirectories.add(patternDirectory);

        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("My phone number is %{CUSTOMPHONENUMBERPATTERN:my_number}"));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "My phone number is 123-456-789");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "My phone number is 123-456-789");
        resultData.put("my_number", "123-456-789");

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.PATTERNS_DIRECTORIES, patternsDirectories);
        pluginSetting.getSettings().put(GrokProcessorConfig.PATTERNS_FILES_GLOB, "*1.txt");
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);

        final Map<String, List<String>> matchConfigWithPatterns2Pattern = new HashMap<>();
        matchConfigWithPatterns2Pattern.put("message", Collections.singletonList("My birthday is %{CUSTOMBIRTHDAYPATTERN:my_birthday}"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfigWithPatterns2Pattern);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);

        Throwable throwable = assertThrows(InvalidPluginConfigurationException.class, () -> new GrokProcessor(
                pluginMetrics, grokProcessorConfig, expressionEvaluator));
        assertThat(throwable.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat("No definition for key 'CUSTOMBIRTHDAYPATTERN' found, aborting", equalTo(throwable
                .getCause().getMessage()));
    }

    @Test
    public void testMatchWithNamedCapturesSyntax() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{GREEDYDATA:greedy_data} (?<mynumber>\\d\\d\\d-\\d\\d\\d-\\d\\d\\d)"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "This is my greedy data before matching with my phone number 123-456-789");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "This is my greedy data before matching with my phone number 123-456-789");
        resultData.put("greedy_data", "This is my greedy data before matching with my phone number");
        resultData.put("mynumber", "123-456-789");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testMatchWithNoCapturesAndTags() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{GREEDYDATA:greedy_data} (?<mynumber>\\d\\d\\d-\\d\\d\\d-\\d\\d\\d)"));
        final String tagOnMatchFailure1 = UUID.randomUUID().toString();
        final String tagOnMatchFailure2 = UUID.randomUUID().toString();

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokProcessorConfig.TAGS_ON_MATCH_FAILURE, List.of(tagOnMatchFailure1, tagOnMatchFailure2));
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("log", "This is my greedy data before matching with my phone number 123-456-789");

        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertRecordsAreEqual(grokkedRecords.get(0), record);
        assertTrue(((Event)record.getData()).getMetadata().getTags().contains(tagOnMatchFailure1));
        assertTrue(((Event)record.getData()).getMetadata().getTags().contains(tagOnMatchFailure2));
    }

    @Test
    public void testCompileNonRegisteredPatternThrowsIllegalArgumentException() {

        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{NONEXISTENTPATTERN}"));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);

        assertThrows(InvalidPluginConfigurationException.class, () -> new GrokProcessor(
                pluginMetrics, grokProcessorConfig, expressionEvaluator));
    }

    @ParameterizedTest
    @MethodSource("getGrokPatternInputAndOutput")
    void testDataPrepperBuiltInGrokPatterns(final String matchPattern, final String logInput, final String expectedGrokResultJson) throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList(matchPattern));

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);
        grokProcessorConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), GrokProcessorConfig.class);
        grokProcessor = new GrokProcessor(pluginMetrics, grokProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", logInput);

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> expectedGrokResult = OBJECT_MAPPER.readValue(expectedGrokResultJson, MAP_TYPE_REFERENCE);
        expectedGrokResult.put("message", logInput);


        final Record<Event> resultRecord = buildRecordWithEvent(expectedGrokResult);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    private void assertRecordsAreEqual(final Record<Event> first, final Record<Event> second) throws JsonProcessingException {
        final Map<String, Object> recordMapFirst = OBJECT_MAPPER.readValue(first.getData().toJsonString(), MAP_TYPE_REFERENCE);
        final Map<String, Object> recordMapSecond = OBJECT_MAPPER.readValue(second.getData().toJsonString(), MAP_TYPE_REFERENCE);

        for (final Map.Entry<String, Object> entry : recordMapSecond.entrySet()) {
            assertThat(recordMapFirst, hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    private static Stream<Arguments> getGrokPatternInputAndOutput() {
        return Stream.of(
                Arguments.of("%{VPC_FLOW_LOG}",
                        "2 123456789010 eni-1235b8ca123456789 203.0.113.12 172.31.16.139 0 0 1 4 336 1432917027 1432917142 ACCEPT OK",
                        "{\"srcaddr\":\"203.0.113.12\",\"dstport\":0,\"account-id\":\"123456789010\",\"start\":1432917027,\"dstaddr\":\"172.31.16.139\",\"version\":\"2\",\"packets\":4,\"protocol\":1,\"bytes\":336,\"srcport\":0,\"action\":\"ACCEPT\",\"end\":1432917142,\"log-status\":\"OK\",\"interface-id\":\"eni-1235b8ca123456789\"}"
                ),
                Arguments.of("%{VPC_FLOW_LOG}",
                        "2 123456789010 eni-1235b8ca123456789 - - - - - - - 1431280876 1431280934 - NODATA",
                        "{\"srcaddr\":\"-\",\"account-id\":\"123456789010\",\"start\":1431280876,\"action\":\"-\",\"dstaddr\":\"-\",\"end\":1431280934,\"log-status\":\"NODATA\",\"version\":\"2\",\"interface-id\":\"eni-1235b8ca123456789\"}"
                ),
                Arguments.of("%{ALB_ACCESS_LOG}",
                        "https 2017-11-20T22:05:36 long-bill-lb 77.222.19.149:41148 10.168.203.134:23662 0.000201 0.401924 0.772005 500 200 262 455 \"GET https://elmagek.no-ip.org:443/json/v1/collector/histogram/100105037?startTimestamp=1405571270000&endTimestamp=1405574870000&bucketCount=60&_=1405574870206 HTTP/1.1\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.4) Gecko/2008102920 Firefox/3.0.4\" DH-RSA-AES256-GCM-SHA384 TLSv1.2 arn:aws:elasticloadbalancing:us-west-2:104030218370:targetgroup/Prod-frontend/92e3199b1rc814fe9 \"Root=1-58337364-23a8c76965a2ef7629b185e134\" \"my-domain\" \"my-chosen-cert-arn\" 1000 2018-11-20T22:05:36 \"my-action\" \"my-redirect-url\" \"my-error-reason\" \"target:port1 target:port2\" \"target1 target2\" \"class\" \"reason\"",
                        "{\"sent_bytes\":455,\"request_processing_time\":\"0.000201\",\"ssl_protocol\":\"TLSv1.2\",\"actions_executed\":\"my-action\",\"trace_id\":\"Root=1-58337364-23a8c76965a2ef7629b185e134\",\"elb\":\"long-bill-lb\",\"received_bytes\":262,\"domain\":\"elmagek.no-ip.org\",\"response_processing_time\":\"0.772005\",\"domain_name\":\"my-domain\",\"classification_reason\":\"reason\",\"ssl_cipher\":\"DH-RSA-AES256-GCM-SHA384\",\"redirect_url\":\"my-redirect-url\",\"target_processing_time\":\"0.401924\",\"target_group_arn\":\"arn:aws:elasticloadbalancing:us-west-2:104030218370:targetgroup/Prod-frontend/92e3199b1rc814fe9\",\"user_agent\":\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.4) Gecko/2008102920 Firefox/3.0.4\",\"target_status_code\":\"200\",\"http_method\":\"GET\",\"matched_rule_priority\":1000,\"http_version\":\"HTTP/1.1\",\"http_port\":\"443\",\"client\":\"77.222.19.149:41148\",\"error_reason\":\"my-error-reason\",\"target_list\":\"target:port1 target:port2\",\"target\":\"10.168.203.134:23662\",\"classification\":\"class\",\"time\":\"2017-11-20T22:05:36\",\"type\":\"https\",\"request_uri\":\"/json/v1/collector/histogram/100105037?startTimestamp=1405571270000&endTimestamp=1405574870000&bucketCount=60&_=1405574870206\",\"request_creation_time\":\"2018-11-20T22:05:36\",\"target_status_code_list\":\"target1 target2\",\"protocol\":\"https\",\"elb_status_code\":\"500\",\"chosen_cert_arn\":\"my-chosen-cert-arn\"}"
                ),
                Arguments.of("%{ALB_ACCESS_LOG_GENERAL_URI}",
                        "https 2017-11-20T22:05:36 long-bill-lb 77.222.19.149:41148 10.168.203.134:23662 0.000201 0.401924 0.772005 500 200 262 455 \"GET https://elmagek.no-ip.org:443/json/v1/collector/histogram/100105037?startTimestamp=1405571270000&endTimestamp=1405574870000&bucketCount=60&_=1405574870206 HTTP/1.1\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.4) Gecko/2008102920 Firefox/3.0.4\" DH-RSA-AES256-GCM-SHA384 TLSv1.2 arn:aws:elasticloadbalancing:us-west-2:104030218370:targetgroup/Prod-frontend/92e3199b1rc814fe9 \"Root=1-58337364-23a8c76965a2ef7629b185e134\" \"my-domain\" \"my-chosen-cert-arn\" 1000 2018-11-20T22:05:36 \"my-action\" \"my-redirect-url\" \"my-error-reason\" \"target:port1 target:port2\" \"target1 target2\" \"class\" \"reason\"",
                        "{\"request_processing_time\":\"0.000201\",\"ssl_protocol\":\"TLSv1.2\",\"actions_executed\":\"my-action\",\"trace_id\":\"Root=1-58337364-23a8c76965a2ef7629b185e134\",\"elb\":\"long-bill-lb\",\"received_bytes\":262,\"response_processing_time\":\"0.772005\",\"domain_name\":\"my-domain\",\"classification_reason\":\"reason\",\"ssl_cipher\":\"DH-RSA-AES256-GCM-SHA384\",\"redirect_url\":\"my-redirect-url\",\"target_processing_time\":\"0.401924\",\"target_group_arn\":\"arn:aws:elasticloadbalancing:us-west-2:104030218370:targetgroup/Prod-frontend/92e3199b1rc814fe9\",\"user_agent\":\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.4) Gecko/2008102920 Firefox/3.0.4\",\"target_status_code\":\"200\",\"http_method\":\"GET\",\"matched_rule_priority\":1000,\"http_version\":\"HTTP/1.1\",\"http_uri\":\"https://elmagek.no-ip.org:443/json/v1/collector/histogram/100105037?startTimestamp=1405571270000&endTimestamp=1405574870000&bucketCount=60&_=1405574870206\",\"client\":\"77.222.19.149:41148\",\"error_reason\":\"my-error-reason\",\"target_list\":\"target:port1 target:port2\",\"target\":\"10.168.203.134:23662\",\"classification\":\"class\",\"time\":\"2017-11-20T22:05:36\",\"type\":\"https\",\"request_creation_time\":\"2018-11-20T22:05:36\",\"target_status_code_list\":\"target1 target2\",\"elb_status_code\":\"500\",\"chosen_cert_arn\":\"my-chosen-cert-arn\",\"sent_bytes\":455}"
                ),
                Arguments.of("%{S3_ACCESS_LOG}",
                        "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be DOC-EXAMPLE-BUCKET1 [06/Feb/2019:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - \"GET /DOC-EXAMPLE-BUCKET1?versioning HTTP/1.1\" 200 - 113 - 7 - \"-\" \"S3Console/0.4\" - s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader DOC-EXAMPLE-BUCKET1.s3.us-west-1.amazonaws.com TLSV1.2 arn:aws:s3:us-west-1:123456789012:accesspoint/example-AP Yes",
                        "{\"httpversion\":\"1.1\",\"request\":\"/DOC-EXAMPLE-BUCKET1?versioning\",\"timestamp\":\"06/Feb/2019:00:00:38 +0000\",\"requester\":\"79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be\",\"agent\":\"S3Console/0.4\",\"key\":\"-\",\"clientip\":\"192.0.2.3\",\"response\":200,\"operation\":\"REST.GET.VERSIONING\",\"verb\":\"GET\",\"request_id\":\"3E57427F3EXAMPLE\",\"bucket\":\"DOC-EXAMPLE-BUCKET1\",\"owner\":\"79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be\",\"referrer\":\"-\",\"bytes_sent\":113,\"request_time_ms\":7}"
                ),
                Arguments.of("%{ELB_ACCESS_LOG}",
                        "2020-06-14T17:26:04.805368Z my-clb-1 170.01.01.02:39492 172.31.25.183:5000 0.000032 0.001861 0.000017 200 200 0 13 \"GET http://my-clb-1-1798137604.us-east-2.elb.amazonaws.com:80/ HTTP/1.1\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36\" - -",
                        "{\"backendport\":5000,\"received_bytes\":0,\"request\":\"http://my-clb-1-1798137604.us-east-2.elb.amazonaws.com:80/\",\"backend_response\":200,\"verb\":\"GET\",\"clientport\":39492,\"request_processing_time\":3.2E-5,\"urihost\":\"my-clb-1-1798137604.us-east-2.elb.amazonaws.com:80\",\"response_processing_time\":1.7E-5,\"path\":\"/\",\"port\":\"80\",\"response\":200,\"bytes\":13,\"clientip\":\"170.01.01.02\",\"proto\":\"http\",\"elb\":\"my-clb-1\",\"httpversion\":\"1.1\",\"backendip\":\"172.31.25.183\",\"backend_processing_time\":0.001861,\"timestamp\":\"2020-06-14T17:26:04.805368Z\"}"
                )
        );
    }
}
