/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.awaitility.Awaitility.await;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import org.opensearch.dataprepper.model.sink.SinkContext;

import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.RequestThrottledException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SqsSinkServiceTest {
    @Mock
    private SqsClient sqsClient;
    @Mock
    private SqsSinkConfig sqsSinkConfig;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private DlqPushHandler dlqPushHandler;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private SendMessageBatchResponse flushResponse;
    @Mock
    private EventHandle eventHandle;
    @Mock
    private SqsThresholdConfig thresholdConfig;

    @Mock
    private SinkContext sinkContext;

    @Mock
    private Counter eventsSuccessCounter;
    @Mock
    private Counter requestsSuccessCounter;
    @Mock
    private Counter eventsFailedCounter;
    @Mock
    private Counter requestsFailedCounter;
    @Mock
    private Counter dlqSuccessCounter;
    private AtomicInteger eventsSuccessCount;
    private AtomicInteger requestsSuccessCount;
    private AtomicInteger eventsFailedCount;
    private AtomicInteger requestsFailedCount;
    private AtomicInteger dlqSuccessCount;

    private OutputCodec outputCodec;
    private OutputCodecContext outputCodecContext;
    private String queueUrl;

    private SqsSinkService createObjectUnderTest() {
        return new SqsSinkService(sqsSinkConfig, sqsClient, expressionEvaluator, outputCodec, sinkContext, dlqPushHandler, pluginMetrics);
    }

    @BeforeEach
    void setup() {
        sinkContext = mock(SinkContext.class);
        pluginFactory = mock(PluginFactory.class);
        when(sinkContext.getExcludeKeys()).thenReturn(null);
        when(sinkContext.getIncludeKeys()).thenReturn(null);
        when(sinkContext.getTagsTargetKey()).thenReturn(null);
        eventsSuccessCount = new AtomicInteger(0);
        requestsSuccessCount = new AtomicInteger(0);
        eventsFailedCount = new AtomicInteger(0);
        requestsFailedCount = new AtomicInteger(0);
        dlqSuccessCount = new AtomicInteger(0);
        outputCodec = new JsonOutputCodec(new JsonOutputCodecConfig());
        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenReturn(outputCodec);
        eventHandle = mock(EventHandle.class);
        outputCodecContext = new OutputCodecContext();
        queueUrl = UUID.randomUUID().toString();
        sqsSinkConfig = mock(SqsSinkConfig.class);
        thresholdConfig = mock(SqsThresholdConfig.class);
        when (sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl);
        when (thresholdConfig.getMaxMessageSizeBytes()).thenReturn(256*1024L);
        when (thresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        when (sqsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
        when (sqsSinkConfig.getMaxRetries()).thenReturn(3);
        sqsClient = mock(SqsClient.class);
        flushResponse = mock(SendMessageBatchResponse.class);
        when(flushResponse.hasFailed()).thenReturn(false);
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(flushResponse);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidFormatExpression(anyString())).thenReturn(true);
        dlqPushHandler = mock(DlqPushHandler.class);
        when(dlqPushHandler.perform(any(List.class))).thenReturn(true);
        PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn("name");
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(dlqPushHandler.getPluginSetting()).thenReturn(pluginSetting);
        pluginMetrics = mock(PluginMetrics.class);
        eventsSuccessCounter = mock(Counter.class);
        eventsFailedCounter = mock(Counter.class);
        requestsSuccessCounter = mock(Counter.class);
        requestsFailedCounter = mock(Counter.class);
        dlqSuccessCounter = mock(Counter.class);
        lenient().doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            eventsSuccessCount.addAndGet(v);
            return null;
        }).when(eventsSuccessCounter).increment(any(Double.class));
        lenient().doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            eventsFailedCount.addAndGet(v);
            return null;
        }).when(eventsFailedCounter).increment(any(Double.class));
        lenient().doAnswer((a)-> {
            requestsSuccessCount.addAndGet(1);
            return null;
        }).when(requestsSuccessCounter).increment();
        lenient().doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            requestsSuccessCount.addAndGet(v);
            return null;
        }).when(requestsSuccessCounter).increment(any(Double.class));
        lenient().doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            requestsFailedCount.addAndGet(v);
            return null;
        }).when(requestsFailedCounter).increment(any(Double.class));
        lenient().doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            dlqSuccessCount.addAndGet(v);
            return null;
        }).when(dlqSuccessCounter).increment(any(Double.class));
        lenient().doAnswer(a -> {
            String s = (String)(a.getArgument(0));
            if (s.equals(SqsSinkMetrics.SQS_SINK_REQUESTS_SUCCEEDED)) {
                return requestsSuccessCounter;
            }
            if (s.equals(SqsSinkMetrics.SQS_SINK_EVENTS_SUCCEEDED)) {
                return eventsSuccessCounter;
            }
            if (s.equals(SqsSinkMetrics.SQS_SINK_REQUESTS_FAILED)) {
                return requestsFailedCounter;
            }
            if (s.equals(SqsSinkMetrics.SQS_SINK_EVENTS_FAILED)) {
                return eventsFailedCounter;
            }
            if (s.contains("NumDlqSuccess")) {
                return dlqSuccessCounter;
            }
            return null;
        }).when(pluginMetrics).counter(anyString());
    }

    @Test
    void TestBasic() {
        SqsSinkService sqsSinkService = createObjectUnderTest();
        assertTrue(sqsSinkService.exceedsMaxEventSizeThreshold(256*1024+1));
        assertFalse(sqsSinkService.exceedsMaxEventSizeThreshold(256*1024-1));
        assertFalse(sqsSinkService.exceedsMaxEventSizeThreshold(256*1024));
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 29, 49, 69})
    void TestExecuteWithOneBatch_FlushTimeout(int numRecords) throws Exception {
        when (thresholdConfig.getFlushInterval()).thenReturn(2L);
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        sqsSinkService.execute(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
            sqsSinkService.execute(Collections.emptyList());
            assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
            assertThat(eventsSuccessCount.get(), equalTo(numRecords));
            assertThat(requestsSuccessCount.get(), equalTo((numRecords+1)/10));
            verify(eventHandle, times(numRecords)).release(true);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestExecuteOneBatch_WithLargeRecords(int numRecords) throws Exception {
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getLargeRecordList(numRecords);
        sqsSinkService.execute(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
            sqsSinkService.execute(Collections.emptyList());
            assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
            assertThat(eventsSuccessCount.get(), equalTo(numRecords));
            assertThat(requestsSuccessCount.get(), equalTo(numRecords));
            verify(eventHandle, times(numRecords)).release(true);
        });
    }

    @Test
    void TestLargeRecordToDLQ() {
        SqsSinkService sqsSinkService = createObjectUnderTest();
        Record<Event> record = getLargeRecord(300*1024);
        sqsSinkService.execute(List.of(record));
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(0));
        assertThat(requestsSuccessCount.get(), equalTo(0));
    }

    @ParameterizedTest
    @ValueSource(ints = {20, 40, 60, 80})
    void TestExecuteWithOneBatch_SuccessfulFlush_DynamicQUrl(int numRecords) throws Exception {
        when (sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl+"${/id}");
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        sqsSinkService.execute(records);
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestExecuteWithOneBatch_SuccessfulFlush(int numRecords) throws Exception {
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        sqsSinkService.execute(records);
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        verify(eventHandle, times(numRecords)).release(true);
    }
    
    @Test
    void TestSendingToDLQAfterMultipleRetries() {
        final int numRecords = 10;
        RequestThrottledException requestThrottledException = mock(RequestThrottledException.class);
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(requestThrottledException);
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        sqsSinkService.execute(records);
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(0));
        assertThat(requestsSuccessCount.get(), equalTo(0));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestExecuteWithOneBatch_MultipleRetries(int numRecords) throws Exception {
        RequestThrottledException requestThrottledException = mock(RequestThrottledException.class);
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(requestThrottledException).thenReturn(flushResponse);
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        sqsSinkService.execute(records);
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @Test
    void TestFiFoQWithInvalidDeDupIdExpression() {
        when(expressionEvaluator.isValidFormatExpression(anyString())).thenReturn(false);
        when (sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl+".fifo");
        when (sqsSinkConfig.getDeDuplicationId()).thenReturn(UUID.randomUUID().toString()+"${/id - }");
        assertThrows(IllegalArgumentException.class, ()-> createObjectUnderTest());
    }

    @Test
    void TestFiFoQWithInvalidGroupIdExpression() {
        when(expressionEvaluator.isValidFormatExpression(anyString())).thenReturn(false);
        when (sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl+".fifo");
        when (sqsSinkConfig.getGroupId()).thenReturn(UUID.randomUUID().toString()+"${/id - }");
        assertThrows(IllegalArgumentException.class, ()-> createObjectUnderTest());
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestWithOneBatch_SuccessfulFlushFiFoQDynamic(int numRecords) throws Exception {
        when (sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl+".fifo");
        when (sqsSinkConfig.getGroupId()).thenReturn(UUID.randomUUID().toString()+"${/id}");
        when (sqsSinkConfig.getDeDuplicationId()).thenReturn(UUID.randomUUID().toString()+"${/id}");
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        boolean isFull = false;
        for (int i = 0; i < numRecords; i++) {
            assertFalse(isFull);
            Event event = records.get(i).getData();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            isFull = sqsSinkService.addToBuffer(event, eSize);
            if (isFull) {
                Object flushResult = sqsSinkService.doFlushOnce(null);
                assertThat(flushResult, equalTo(null));
                isFull = false;
            }
        }
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        verify(eventHandle, times(numRecords)).release(true);
    }



    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestWithOneBatch_SuccessfulFlushFiFoQ(int numRecords) throws Exception {
        when (sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl+".fifo");
        when (sqsSinkConfig.getGroupId()).thenReturn(UUID.randomUUID().toString());
        when (sqsSinkConfig.getDeDuplicationId()).thenReturn(UUID.randomUUID().toString());
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        boolean isFull = false;
        for (int i = 0; i < numRecords; i++) {
            assertFalse(isFull);
            Event event = records.get(i).getData();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            isFull = sqsSinkService.addToBuffer(event, eSize);
            if (isFull) {
                Object flushResult = sqsSinkService.doFlushOnce(null);
                assertThat(flushResult, equalTo(null));
                isFull = false;
            }
        }
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        verify(eventHandle, times(numRecords)).release(true);
    }


    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestWithOneBatch_SuccessfulFlush(int numRecords) throws Exception {
        SqsSinkService sqsSinkService = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        boolean isFull = false;
        for (int i = 0; i < numRecords; i++) {
            assertFalse(isFull);
            Event event = records.get(i).getData();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            isFull = sqsSinkService.addToBuffer(event, eSize);
            if (isFull) {
                Object flushResult = sqsSinkService.doFlushOnce(null);
                assertThat(flushResult, equalTo(null));
                isFull = false;
            }
        }
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @Test
    void TestWithOneBatch_RetryFlushes() throws Exception {
        RequestThrottledException requestThrottledException = mock(RequestThrottledException.class);
        SqsSinkService sqsSinkService = createObjectUnderTest();
        int numRecords = SqsSinkBatch.MAX_MESSAGES_PER_BATCH;
        List<Record<Event>> records = getRecordList(numRecords);
        boolean isFull = false;
        for (int i = 0; i < numRecords; i++) {
            when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(requestThrottledException);
            assertFalse(isFull);
            Event event = records.get(i).getData();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            isFull = sqsSinkService.addToBuffer(event, eSize);
            if (isFull) {
                Object flushResult = sqsSinkService.doFlushOnce(null);
                assertThat(flushResult, not(equalTo(null)));
                assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(1)); 
                when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(flushResponse);
                 flushResult = sqsSinkService.doFlushOnce(null);
                assertThat(flushResult, equalTo(null));
            }
        }
        assertThat(sqsSinkService.getBatchUrlMap().size(), equalTo(0)); 
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        verify(eventHandle, times(numRecords)).release(true);
    }

    private List<Record<Event>> getLargeRecordList(int numberOfRecords) {
        final List<Record<Event>> recordList = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            recordList.add(getLargeRecord(245*1024));
        }
        return recordList;
    }

    private Record<Event> getLargeRecord(int size) {
        final Event event = JacksonLog.builder()
                            .withData(Map.of("key", RandomStringUtils.randomAlphabetic(size)))
                            .withEventHandle(eventHandle)
                            .build();
        return new Record<>(event);
    }

    private List<Record<Event>> getRecordList(int numberOfRecords) {
        final List<Record<Event>> recordList = new ArrayList<>();
        List<HashMap> records = generateRecords(numberOfRecords);
        for (int i = 0; i < numberOfRecords; i++) {
            final Event event = JacksonLog.builder().withData(records.get(i)).withEventHandle(eventHandle).build();
            recordList.add(new Record<>(event));
        }
        return recordList;
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, String> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            eventData.put("id", Integer.toString(rows%2));
            recordList.add(eventData);

        }
        return recordList;
    }

}
