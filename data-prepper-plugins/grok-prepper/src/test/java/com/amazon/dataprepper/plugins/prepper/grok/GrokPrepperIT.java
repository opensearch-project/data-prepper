package com.amazon.dataprepper.plugins.prepper.grok;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperTests.assertRecordsAreEqual;
import static com.amazon.dataprepper.plugins.prepper.grok.GrokPrepperTests.buildRecordWithEvent;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GrokPrepperIT {
    private PluginSetting pluginSetting;
    private GrokPrepper grokPrepper;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private final String PLUGIN_NAME = "grok";
    private String messageInput;

    @BeforeEach
    public void setup() {

        pluginSetting = completePluginSettingForGrokPrepper(
                GrokPrepperConfig.DEFAULT_BREAK_ON_MATCH,
                GrokPrepperConfig.DEFAULT_KEEP_EMPTY_CAPTURES,
                Collections.emptyMap(),
                GrokPrepperConfig.DEFAULT_NAMED_CAPTURES_ONLY,
                Collections.emptyList(),
                Collections.emptyList(),
                GrokPrepperConfig.DEFAULT_PATTERNS_FILES_GLOB,
                Collections.emptyMap(),
                GrokPrepperConfig.DEFAULT_TIMEOUT_MILLIS,
                GrokPrepperConfig.DEFAULT_TARGET_KEY);

        pluginSetting.setPipelineName("grokPipeline");

        // This is a COMMONAPACHELOG pattern with the following format
        // COMMONAPACHELOG %{IPORHOST:clientip} %{USER:ident} %{USER:auth} \[%{HTTPDATE:timestamp}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})" %{NUMBER:response} (?:%{NUMBER:bytes}|-)
        // Note that rawrequest is missing from the log below, which means that it will not be captured unless keep_empty_captures is true
        messageInput = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";
    }

    @AfterEach
    public void tearDown() {
        grokPrepper.shutdown();
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

    @Test
    public void testMatchNoCaptures() throws JsonProcessingException {
        final String nonMatchingPattern = "%{SYSLOGBASE}";
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList(nonMatchingPattern));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        grokPrepper = new GrokPrepper(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);

        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), record);
    }

    @Test
    public void testSingleMatchSinglePatternWithDefaults() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{COMMONAPACHELOG}"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        grokPrepper = new GrokPrepper(pluginSetting);

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

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

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

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokPrepperConfig.BREAK_ON_MATCH, false);
        grokPrepper = new GrokPrepper(pluginSetting);

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

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testSingleMatchTypeConversionWithDefaults() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("\"(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\" %{NUMBER:response:int} (?:%{NUMBER:bytes:float}|-)"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        grokPrepper = new GrokPrepper(pluginSetting);

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

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testMultipleMatchWithBreakOnMatchFalse() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{COMMONAPACHELOG}"));
        matchConfig.put("extra_field", Collections.singletonList("%{GREEDYDATA} %{IPORHOST:host}"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokPrepperConfig.BREAK_ON_MATCH, false);
        grokPrepper = new GrokPrepper(pluginSetting);

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

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testMatchWithKeepEmptyCapturesTrue() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{COMMONAPACHELOG}"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokPrepperConfig.KEEP_EMPTY_CAPTURES, true);
        grokPrepper = new GrokPrepper(pluginSetting);

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

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testMatchWithNamedCapturesOnlyFalse() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{GREEDYDATA} %{IPORHOST:host} %{NUMBER}"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokPrepperConfig.NAMED_CAPTURES_ONLY, false);
        grokPrepper = new GrokPrepper(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "This is my greedy data before matching 192.0.2.1 123456");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "This is my greedy data before matching 192.0.2.1 123456");
        resultData.put("NUMBER", "123456");
        resultData.put("GREEDYDATA", "This is my greedy data before matching");
        resultData.put("host", "192.0.2.1");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

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

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokPrepperConfig.PATTERN_DEFINITIONS, patternDefinitions);
        grokPrepper = new GrokPrepper(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "This is my greedy data before matching with my phone number 123-456-789");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "This is my greedy data before matching with my phone number 123-456-789");
        resultData.put("greedy_data", "This is my greedy data before matching with my phone number");
        resultData.put("my_number", "123-456-789");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testPatternsDirWithDefaultPatternsFilesGlob() throws JsonProcessingException {
        final String patternDirectory = "./src/test/resources/patterns";

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

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokPrepperConfig.PATTERNS_DIRECTORIES, patternsDirectories);
        grokPrepper = new GrokPrepper(pluginSetting);

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testPatternsDirWithCustomPatternsFilesGlob() throws JsonProcessingException {
        final String patternDirectory = "./src/test/resources/patterns";

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

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        pluginSetting.getSettings().put(GrokPrepperConfig.PATTERNS_DIRECTORIES, patternsDirectories);
        pluginSetting.getSettings().put(GrokPrepperConfig.PATTERNS_FILES_GLOB, "*1.txt");
        grokPrepper = new GrokPrepper(pluginSetting);

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);

        final Map<String, List<String>> matchConfigWithPatterns2Pattern = new HashMap<>();
        matchConfigWithPatterns2Pattern.put("message", Collections.singletonList("My birthday is %{CUSTOMBIRTHDAYPATTERN:my_birthday}"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfigWithPatterns2Pattern);

        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> new GrokPrepper((pluginSetting)));
        assertThat("No definition for key 'CUSTOMBIRTHDAYPATTERN' found, aborting", equalTo(throwable.getMessage()));
    }

    @Test
    public void testMatchWithNamedCapturesSyntax() throws JsonProcessingException {
        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{GREEDYDATA:greedy_data} (?<mynumber>\\d\\d\\d-\\d\\d\\d-\\d\\d\\d)"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);
        grokPrepper = new GrokPrepper(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "This is my greedy data before matching with my phone number 123-456-789");

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "This is my greedy data before matching with my phone number 123-456-789");
        resultData.put("greedy_data", "This is my greedy data before matching with my phone number");
        resultData.put("mynumber", "123-456-789");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
    }

    @Test
    public void testCompileNonRegisteredPatternThrowsIllegalArgumentException() {

        grokPrepper = new GrokPrepper(pluginSetting);

        final Map<String, List<String>> matchConfig = new HashMap<>();
        matchConfig.put("message", Collections.singletonList("%{NONEXISTENTPATTERN}"));

        pluginSetting.getSettings().put(GrokPrepperConfig.MATCH, matchConfig);

        assertThrows(IllegalArgumentException.class, () -> new GrokPrepper(pluginSetting));
    }

}
