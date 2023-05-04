/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.grok;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.grok.GrokProcessor.EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT;
import static org.opensearch.dataprepper.test.matcher.MapEquals.isEqualWithoutTimestamp;


@ExtendWith(MockitoExtension.class)
public class GrokProcessorTests {
    private GrokProcessor grokProcessor;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private String messageInput;

    @Mock
    private ExecutorService executorService;

    @Mock
    private Future task;

    @Mock
    private GrokCompiler grokCompiler;

    @Mock
    private Match match;

    @Mock
    private Grok grok;

    @Mock
    private Grok grokSecondMatch;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter grokProcessingMatchCounter;

    @Mock
    private Counter grokProcessingMismatchCounter;

    @Mock
    private Counter grokProcessingErrorsCounter;

    @Mock
    private Counter grokProcessingTimeoutsCounter;

    @Mock
    private Timer grokProcessingTime;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    private PluginSetting pluginSetting;
    private final String PLUGIN_NAME = "grok";
    private Map<String, Object> capture;
    private final Map<String, List<String>> matchConfig = new HashMap<>();

    @BeforeEach
    public void setup() throws TimeoutException, ExecutionException, InterruptedException {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("grokPipeline");

        final List<String> matchPatterns = new ArrayList<>();
        matchPatterns.add("%{PATTERN1}");
        matchPatterns.add("%{PATTERN2}");
        matchConfig.put("message", matchPatterns);

        pluginSetting.getSettings().put(GrokProcessorConfig.MATCH, matchConfig);

        lenient().when(pluginMetrics.counter(GrokProcessor.GROK_PROCESSING_MATCH)).thenReturn(grokProcessingMatchCounter);
        lenient().when(pluginMetrics.counter(GrokProcessor.GROK_PROCESSING_MISMATCH)).thenReturn(grokProcessingMismatchCounter);
        lenient().when(pluginMetrics.counter(GrokProcessor.GROK_PROCESSING_TIMEOUTS)).thenReturn(grokProcessingTimeoutsCounter);
        lenient().when(pluginMetrics.counter(GrokProcessor.GROK_PROCESSING_ERRORS)).thenReturn(grokProcessingErrorsCounter);
        lenient().when(pluginMetrics.timer(GrokProcessor.GROK_PROCESSING_TIME)).thenReturn(grokProcessingTime);

        lenient().doAnswer(a -> {
            a.<Runnable>getArgument(0).run();
            return null;
        }).when(grokProcessingTime).record(any(Runnable.class));

        capture = new HashMap<>();

        messageInput = UUID.randomUUID().toString();

        lenient().when(grokCompiler.compile(eq(matchConfig.get("message").get(0)), anyBoolean())).thenReturn(grok);
        lenient().when(grokCompiler.compile(eq(matchConfig.get("message").get(1)), anyBoolean())).thenReturn(grokSecondMatch);

        lenient().when(grok.match(messageInput)).thenReturn(match);
        lenient().when(match.capture()).thenReturn(capture);
        lenient().when(executorService.submit(any(Runnable.class))).then(a -> {
            a.<Runnable>getArgument(0).run();
            return task;
        });
        lenient().when(task.get(GrokProcessorConfig.DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)).thenReturn(null);
    }

    private GrokProcessor createObjectUnderTest() {
        try (MockedStatic<PluginMetrics> pluginMetricsMockedStatic = mockStatic(PluginMetrics.class)) {
            pluginMetricsMockedStatic.when(() -> PluginMetrics.fromPluginSetting(pluginSetting)).thenReturn(pluginMetrics);
            return new GrokProcessor(pluginSetting, grokCompiler, executorService, expressionEvaluator);
        }
    }

    @Test
    public void testMatchMerge() throws JsonProcessingException {
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("key_capture_1", "value_capture_1");
        resultData.put("key_capture_2", "value_capture_2");
        resultData.put("key_capture_3", "value_capture_3");;

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }

    @Test
    public void testTarget() throws JsonProcessingException {
        pluginSetting.getSettings().put(GrokProcessorConfig.TARGET_KEY, "test_target");
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> testTarget = new HashMap<>();
        testTarget.put("key_capture_1", "value_capture_1");
        testTarget.put("key_capture_2", "value_capture_2");
        testTarget.put("key_capture_3", "value_capture_3");

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("test_target", testTarget);

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }

    @Test
    public void testOverwrite() throws JsonProcessingException {
        pluginSetting.getSettings().put(GrokProcessorConfig.KEYS_TO_OVERWRITE, Collections.singletonList("message"));
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");
        capture.put("message", "overwrite_the_original_message");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", "overwrite_the_original_message");
        resultData.put("key_capture_1", "value_capture_1");
        resultData.put("key_capture_2", "value_capture_2");
        resultData.put("key_capture_3", "value_capture_3");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }

    @Test
    public void testMatchMergeCollisionStrings() throws JsonProcessingException {
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        testData.put("key_capture_1", "value_capture_collision");
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("key_capture_1", Arrays.asList("value_capture_collision", "value_capture_1"));
        resultData.put("key_capture_2", "value_capture_2");
        resultData.put("key_capture_3", "value_capture_3");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }

    @Test
    public void testMatchMergeCollisionInts() throws JsonProcessingException {
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", 20);
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        testData.put("key_capture_1", 10);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("key_capture_1", Arrays.asList(10, 20));
        resultData.put("key_capture_2", "value_capture_2");
        resultData.put("key_capture_3", "value_capture_3");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }

    @Test
    public void testMatchMergeCollisionWithListMixedTypes() throws JsonProcessingException {
        grokProcessor = createObjectUnderTest();

        List<Object> captureListValues = new ArrayList<>();
        captureListValues.add("30");
        captureListValues.add(40);
        captureListValues.add(null);

        capture.put("key_capture_1", captureListValues);
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        testData.put("key_capture_1", Arrays.asList(10, 20));
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("key_capture_1", Arrays.asList(10, 20, "30", 40, null));
        resultData.put("key_capture_2", "value_capture_2");
        resultData.put("key_capture_3", "value_capture_3");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }

    @Test
    public void testMatchMergeCollisionWithNullValue() throws JsonProcessingException {
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        testData.put("key_capture_1", null);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("key_capture_1", Arrays.asList(null, "value_capture_1"));
        resultData.put("key_capture_2", "value_capture_2");
        resultData.put("key_capture_3", "value_capture_3");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }

    @Test
    public void testThatTimeoutExceptionIsCaughtAndProcessingContinues() throws JsonProcessingException, TimeoutException, ExecutionException, InterruptedException {
        when(task.get(GrokProcessorConfig.DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)).thenThrow(TimeoutException.class);

        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), record);
        verify(grokProcessingTimeoutsCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
    }

    @Test
    public void testThatProcessingWithTimeoutMillisOfZeroDoesNotInteractWithExecutorServiceAndReturnsCorrectResult() throws JsonProcessingException {
        pluginSetting.getSettings().put(GrokProcessorConfig.TIMEOUT_MILLIS, 0);
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);

        final Record<Event> record = buildRecordWithEvent(testData);

        final Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", messageInput);
        resultData.put("key_capture_1", "value_capture_1");
        resultData.put("key_capture_2", "value_capture_2");
        resultData.put("key_capture_3", "value_capture_3");

        final Record<Event> resultRecord = buildRecordWithEvent(resultData);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));
        verifyNoInteractions(executorService);
        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
        verify(grokProcessingMatchCounter, times(1)).increment();
        verify(grokProcessingTime, times(1)).record(any(Runnable.class));
        verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
    }


    @Test
    public void testPrepareForShutdown() {
        grokProcessor = createObjectUnderTest();
        grokProcessor.prepareForShutdown();
    }

    @Test
    public void testShutdown_Successful() throws InterruptedException {
        grokProcessor = createObjectUnderTest();
        lenient().when(executorService.awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.MILLISECONDS))).thenReturn(true);

        grokProcessor.shutdown();
        verify(executorService).shutdown();
        verify(executorService).awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testShutdown_Timeout() throws InterruptedException {
        grokProcessor = createObjectUnderTest();
        lenient().when(executorService.awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.MILLISECONDS))).thenReturn(false);

        grokProcessor.shutdown();
        verify(executorService).shutdown();
        verify(executorService).awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.MILLISECONDS));
        verify(executorService).shutdownNow();
    }

    @Test
    public void testShutdown_InterruptedException() throws InterruptedException {
        grokProcessor = createObjectUnderTest();
        lenient().when(executorService.awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new InterruptedException());

        grokProcessor.shutdown();
        verify(executorService).shutdown();
        verify(executorService).awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.MILLISECONDS));
        verify(executorService).shutdownNow();
    }

    @Nested
    class WithMultipleMatches {
        @Mock
        private Match secondMatch;

        private Map<String, Object> secondCapture;

        @BeforeEach
        public void setup() {
            secondCapture = new HashMap<>();
        }

        @Test
        public void testNoCaptures() throws JsonProcessingException {
            grokProcessor = createObjectUnderTest();

            lenient().when(grokSecondMatch.match(messageInput)).thenReturn(secondMatch);
            lenient().when(secondMatch.capture()).thenReturn(secondCapture);

            final Map<String, Object> testData = new HashMap();
            testData.put("message", messageInput);
            final Record<Event> record = buildRecordWithEvent(testData);

            final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

            assertThat(grokkedRecords.size(), equalTo(1));
            assertThat(grokkedRecords.get(0), notNullValue());
            assertRecordsAreEqual(grokkedRecords.get(0), record);
            verify(grokProcessingMismatchCounter, times(1)).increment();
            verify(grokProcessingTime, times(1)).record(any(Runnable.class));
            verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMatchCounter, grokProcessingTimeoutsCounter);
        }

        @Test
        public void testBreakOnMatchTrue() throws JsonProcessingException {
            grokProcessor = createObjectUnderTest();

            lenient().when(grokSecondMatch.match(messageInput)).thenReturn(secondMatch);
            lenient().when(secondMatch.capture()).thenReturn(secondCapture);

            capture.put("key_capture_1", "value_capture_1");
            capture.put("key_capture_2", "value_capture_2");
            capture.put("key_capture_3", "value_capture_3");

            secondCapture.put("key_secondCapture", "value_capture2");

            final Map<String, Object> testData = new HashMap();
            testData.put("message", messageInput);
            final Record<Event> record = buildRecordWithEvent(testData);

            final Map<String, Object> resultData = new HashMap<>();
            resultData.put("message", messageInput);
            resultData.put("key_capture_1", "value_capture_1");
            resultData.put("key_capture_2", "value_capture_2");
            resultData.put("key_capture_3", "value_capture_3");

            final Record<Event> resultRecord = buildRecordWithEvent(resultData);

            final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

            verifyNoInteractions(grokSecondMatch, secondMatch);
            assertThat(grokkedRecords.size(), equalTo(1));
            assertThat(grokkedRecords.get(0), notNullValue());
            assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
            verify(grokProcessingMatchCounter, times(1)).increment();
            verify(grokProcessingTime, times(1)).record(any(Runnable.class));
            verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
        }

        @Test
        public void testBreakOnMatchFalse() throws JsonProcessingException {
            pluginSetting.getSettings().put(GrokProcessorConfig.BREAK_ON_MATCH, false);
            grokProcessor = createObjectUnderTest();

            when(grokSecondMatch.match(messageInput)).thenReturn(secondMatch);
            when(secondMatch.capture()).thenReturn(secondCapture);

            capture.put("key_capture_1", "value_capture_1");
            capture.put("key_capture_2", "value_capture_2");
            capture.put("key_capture_3", "value_capture_3");

            secondCapture.put("key_secondCapture", "value_secondCapture");

            final Map<String, Object> testData = new HashMap();
            testData.put("message", messageInput);
            final Record<Event> record = buildRecordWithEvent(testData);

            final Map<String, Object> resultData = new HashMap<>();
            resultData.put("message", messageInput);
            resultData.put("key_capture_1", "value_capture_1");
            resultData.put("key_capture_2", "value_capture_2");
            resultData.put("key_secondCapture", "value_secondCapture");
            resultData.put("key_capture_3", "value_capture_3");

            final Record<Event> resultRecord = buildRecordWithEvent(resultData);

            final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

            assertThat(grokkedRecords.size(), equalTo(1));
            assertThat(grokkedRecords.get(0), notNullValue());
            assertRecordsAreEqual(grokkedRecords.get(0), resultRecord);
            verify(grokProcessingMatchCounter, times(1)).increment();
            verify(grokProcessingTime, times(1)).record(any(Runnable.class));
            verifyNoInteractions(grokProcessingErrorsCounter, grokProcessingMismatchCounter, grokProcessingTimeoutsCounter);
        }
    }

    private PluginSetting getDefaultPluginSetting() {

        return completePluginSettingForGrokProcessor(
                GrokProcessorConfig.DEFAULT_BREAK_ON_MATCH,
                GrokProcessorConfig.DEFAULT_KEEP_EMPTY_CAPTURES,
                matchConfig,
                GrokProcessorConfig.DEFAULT_NAMED_CAPTURES_ONLY,
                Collections.emptyList(),
                Collections.emptyList(),
                GrokProcessorConfig.DEFAULT_PATTERNS_FILES_GLOB,
                Collections.emptyMap(),
                GrokProcessorConfig.DEFAULT_TIMEOUT_MILLIS,
                GrokProcessorConfig.DEFAULT_TARGET_KEY,
                null);
    }

    @Test
    public void testNoGrok_when_GrokWhen_returns_false() throws JsonProcessingException {
        final String grokWhen = UUID.randomUUID().toString();
        pluginSetting.getSettings().put(GrokProcessorConfig.GROK_WHEN, grokWhen);
        grokProcessor = createObjectUnderTest();

        capture.put("key_capture_1", "value_capture_1");
        capture.put("key_capture_2", "value_capture_2");
        capture.put("key_capture_3", "value_capture_3");

        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        when(expressionEvaluator.evaluate(grokWhen, record.getData())).thenReturn(false);

        final List<Record<Event>> grokkedRecords = (List<Record<Event>>) grokProcessor.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), notNullValue());
        assertRecordsAreEqual(grokkedRecords.get(0), record);
        verifyNoInteractions(grok, grokSecondMatch);
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
                                                              final String grokWhen) {
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

        return new PluginSetting(PLUGIN_NAME, settings);
    }

     private void assertRecordsAreEqual(final Record<Event> first, final Record<Event> second) throws JsonProcessingException {
        final Map<String, Object> recordMapFirst = OBJECT_MAPPER.readValue(first.getData().toJsonString(), MAP_TYPE_REFERENCE);
        final Map<String, Object> recordMapSecond = OBJECT_MAPPER.readValue(second.getData().toJsonString(), MAP_TYPE_REFERENCE);

        assertThat(recordMapFirst, isEqualWithoutTimestamp(recordMapSecond));
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
