/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.RequestThrottledException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.Matchers.greaterThan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SqsSinkBatchTest {
    @Mock
    private EventHandle eventHandle;
    @Mock
    private SendMessageBatchResponse flushResponse;
    @Mock
    private SqsClient sqsClient;
    @Mock
    private SqsException sqsException;

    private AtomicInteger eventsSuccessCount;
    private AtomicInteger requestsSuccessCount;
    private AtomicInteger eventsFailedCount;
    private AtomicInteger requestsFailedCount;
    private AtomicInteger dlqSuccessCount;
    private Buffer buffer;

    private OutputCodec outputCodec;
    private OutputCodecContext outputCodecContext;

    private String groupId;
    private String deDupId;
    private ObjectMapper objectMapper;
    private SqsSinkMetrics sinkMetrics;
    private InMemoryBufferFactory bufferFactory;
    private long maxMessageSize;
    private int maxEvents;
    private SqsSinkBatch batch;
    private String queueUrl;

    private SqsSinkBatch createObjectUnderTest() {
        return new SqsSinkBatch(bufferFactory, sqsClient, sinkMetrics, queueUrl, outputCodec, outputCodecContext, maxMessageSize, maxEvents);
    }

    @BeforeEach
    void setup() {
        eventsSuccessCount = new AtomicInteger(0);
        requestsSuccessCount = new AtomicInteger(0);
        eventsFailedCount = new AtomicInteger(0);
        requestsFailedCount = new AtomicInteger(0);
        dlqSuccessCount = new AtomicInteger(0);
        objectMapper = new ObjectMapper();
        queueUrl = UUID.randomUUID().toString();
        bufferFactory = new InMemoryBufferFactory();
        sqsClient = mock(SqsClient.class);
        sqsException = mock(SqsException.class);
        sinkMetrics = mock(SqsSinkMetrics.class);
        eventHandle = mock(EventHandle.class);
        lenient().doAnswer((a)-> {
            int v = (int)(a.getArgument(0));
            eventsSuccessCount.addAndGet(v);
            return null;
        }).when(sinkMetrics).incrementEventsSuccessCounter(any(Integer.class));
        lenient().doAnswer((a)-> {
            int v = (int)(a.getArgument(0));
            eventsFailedCount.addAndGet(v);
            return null;
        }).when(sinkMetrics).incrementEventsFailedCounter(any(Integer.class));
        lenient().doAnswer((a)-> {
            int v = (int)(a.getArgument(0));
            requestsSuccessCount.addAndGet(v);
            return null;
        }).when(sinkMetrics).incrementRequestsSuccessCounter(any(Integer.class));
        lenient().doAnswer((a)-> {
            int v = (int)(a.getArgument(0));
            requestsFailedCount.addAndGet(v);
            return null;
        }).when(sinkMetrics).incrementRequestsFailedCounter(any(Integer.class));
        flushResponse = mock(SendMessageBatchResponse.class);
        outputCodec = new JsonOutputCodec(new JsonOutputCodecConfig());
        outputCodecContext = new OutputCodecContext();
        maxMessageSize = 256 * 1024;
    }

    @Test
    void TestBasic() {
        batch = createObjectUnderTest();
        assertThat(batch.getQueueUrl(), equalTo(queueUrl));
        assertThat(batch.getCurrentBatchSize(), equalTo(0L));
        assertThat(batch.getEventCount(), equalTo(0));
        assertThat(batch.getEntries().size(), equalTo(0));
    }

    @Test
    void TestOneBatch_WithOneEventPerMessage_WithSuccessfulSendMessage() throws Exception {
        maxEvents = 1;
        batch = createObjectUnderTest();
        groupId = UUID.randomUUID().toString();
        String dedupId = UUID.randomUUID().toString();

        when(flushResponse.hasFailed()).thenReturn(false);
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(flushResponse);
        final int numRecords = SqsSinkBatch.MAX_MESSAGES_PER_BATCH;
        List<Record<Event>> records = getRecordList(numRecords);
        long minSize = 0;
        for (int i = 0; i <= numRecords; i++) {

            Event event = records.get(i%numRecords).getData();
            minSize += event.toJsonString().length();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            // Make sure trying to add more than max possible records results in an exception
            if (i == numRecords) {
                assertThrows(RuntimeException.class, () -> batch.addEntry(event, groupId, dedupId, eSize));
            } else {
                boolean result = batch.addEntry(event, groupId, dedupId, eSize);
                if (i < numRecords-1) {
                    assertFalse(result);
                } else {
                    assertTrue(result);
                }
            }
        }
        assertThat(batch.getEntries().size(), equalTo(numRecords));
        batch.setFlushReady();
        assertTrue(batch.willExceedLimits(1L));
        assertThat(batch.getCurrentBatchSize(), greaterThan(minSize));
        assertThat(batch.getEventCount(), equalTo(SqsSinkBatch.MAX_MESSAGES_PER_BATCH));
        boolean flushResult = batch.flushOnce(null);
        assertTrue(flushResult);
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @Test
    void TestOneBatch_WithOneEventPerMessage_WithFlushFailure() throws Exception {
        maxEvents = 1;
        batch = createObjectUnderTest();
        groupId = UUID.randomUUID().toString();
        String dedupId = UUID.randomUUID().toString();

        SendMessageBatchResponse flushResponse = mock(SendMessageBatchResponse.class);
        when(flushResponse.hasFailed()).thenReturn(true);
        BatchResultErrorEntry errorEntry = mock(BatchResultErrorEntry.class);
        when(errorEntry.id()).thenReturn(UUID.randomUUID().toString());
        when(errorEntry.message()).thenReturn(UUID.randomUUID().toString());
        when(errorEntry.senderFault()).thenReturn(true);
        when(flushResponse.failed()).thenReturn(List.of(errorEntry));
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(flushResponse);
        final int numRecords = SqsSinkBatch.MAX_MESSAGES_PER_BATCH;
        List<Record<Event>> records = getRecordList(numRecords);
        long minSize = 0;
        for (int i = 0; i < numRecords; i++) {

            Event event = records.get(i).getData();
            minSize += event.toJsonString().length();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            batch.addEntry(event, groupId, dedupId, eSize);
        }
        assertThat(batch.getEntries().size(), equalTo(numRecords));
        batch.setFlushReady();
        assertTrue(batch.willExceedLimits(1L));
        assertThat(batch.getCurrentBatchSize(), greaterThan(minSize));
        assertThat(batch.getEventCount(), equalTo(SqsSinkBatch.MAX_MESSAGES_PER_BATCH));
        boolean flushResult = batch.flushOnce((e, m) -> {});
        // all entries sent to DLQ
        assertTrue(flushResult);
    }


    @Test
    void TestOneBatch_WithOneEventPerMessage_WithFlushExceptionFailure() throws Exception {
        maxEvents = 1;
        batch = createObjectUnderTest();
        groupId = UUID.randomUUID().toString();
        String dedupId = UUID.randomUUID().toString();

        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(RequestThrottledException.builder().build());
        final int numRecords = SqsSinkBatch.MAX_MESSAGES_PER_BATCH;
        List<Record<Event>> records = getRecordList(numRecords);
        long minSize = 0;
        for (int i = 0; i < numRecords; i++) {

            Event event = records.get(i).getData();
            minSize += event.toJsonString().length();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            batch.addEntry(event, groupId, dedupId, eSize);
        }
        assertThat(batch.getEntries().size(), equalTo(numRecords));
        batch.setFlushReady();
        assertTrue(batch.willExceedLimits(1L));
        assertThat(batch.getCurrentBatchSize(), greaterThan(minSize));
        assertThat(batch.getEventCount(), equalTo(SqsSinkBatch.MAX_MESSAGES_PER_BATCH));
        boolean flushResult = batch.flushOnce((e, m) -> {});
        assertFalse(flushResult);
    }

    @Test
    void TestOneBatch_WithOneEventPerMessage_WithFlushException() throws Exception {
        maxEvents = 1;
        batch = createObjectUnderTest();
        groupId = UUID.randomUUID().toString();
        String dedupId = UUID.randomUUID().toString();

        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(sqsException);
        final int numRecords = SqsSinkBatch.MAX_MESSAGES_PER_BATCH;
        List<Record<Event>> records = getRecordList(numRecords);
        long minSize = 0;
        for (int i = 0; i < numRecords; i++) {

            Event event = records.get(i).getData();
            minSize += event.toJsonString().length();
            long eSize = outputCodec.getEstimatedSize(event, new OutputCodecContext());
            batch.addEntry(event, groupId, dedupId, eSize);
        }
        assertThat(batch.getEntries().size(), equalTo(numRecords));
        batch.setFlushReady();
        assertTrue(batch.willExceedLimits(1L));
        assertThat(batch.getCurrentBatchSize(), greaterThan(minSize));
        assertThat(batch.getEventCount(), equalTo(SqsSinkBatch.MAX_MESSAGES_PER_BATCH));
        boolean flushResult = batch.flushOnce((e, m) -> {});
        assertTrue(flushResult);
        assertThat(eventsFailedCount.get(), equalTo(numRecords));
        assertThat(requestsFailedCount.get(), equalTo(1));
        verify(eventHandle, times(0)).release(true);
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
            recordList.add(eventData);

        }
        return recordList;
    }
}


