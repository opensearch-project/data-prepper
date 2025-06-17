/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsClientFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.s3.S3DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriterConfig;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.codec.OutputCodec;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import software.amazon.awssdk.regions.Region;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith(MockitoExtension.class)
public class SqsSinkIT {
    static final int NUM_RECORDS = 10;
    static final int MAX_SIZE = 256*1024;
    static final String DLQ_PREFIX = "sqsSinkIT/";
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private EventHandle eventHandle;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsConfig awsConfig;

    @Mock
    private SqsThresholdConfig thresholdConfig;

    @Mock
    private SqsSinkConfig sqsSinkConfig;

    @Mock
    private SinkContext sinkContext;

    @Mock
    private PluginModel codec;

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
    @Mock
    private DistributionSummary summary;

    private JsonOutputCodec jsonCodec;
    private String bucket;
    private String awsRegion;
    private String awsRole;
    private String queueUrl;
    private String groupId;
    private SqsSink sink;
    private AtomicInteger count;
    private SqsClient sqsClient;
    private ObjectMapper objectMapper;
    private ExpressionEvaluator expressionEvaluator;
    private List<Message> messages;
    private AtomicInteger eventsSuccessCount;
    private AtomicInteger requestsSuccessCount;
    private AtomicInteger eventsFailedCount;
    private AtomicInteger requestsFailedCount;
    private AtomicInteger dlqSuccessCount;
    private AwsCredentialsProvider awsCredentialsProvider;
    private S3Client s3Client;
    private int numLargeMessages;
    private Random random;


    @BeforeEach
    void setUp() {
        random = new Random();
        numLargeMessages = 0;
        awsCredentialsProvider = DefaultCredentialsProvider.create();
        pluginMetrics = mock(PluginMetrics.class);
        eventsSuccessCount = new AtomicInteger(0);
        requestsSuccessCount = new AtomicInteger(0);
        eventsFailedCount = new AtomicInteger(0);
        requestsFailedCount = new AtomicInteger(0);
        dlqSuccessCount = new AtomicInteger(0);
        eventsSuccessCounter = mock(Counter.class);
        eventsFailedCounter = mock(Counter.class);
        requestsSuccessCounter = mock(Counter.class);
        requestsFailedCounter = mock(Counter.class);
        dlqSuccessCounter = mock(Counter.class);
        summary = mock(DistributionSummary.class);
        doNothing().when(summary).record(any(Double.class));
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
        when(pluginMetrics.summary(anyString())).thenReturn(summary);
        messages = new ArrayList<>();
        pluginFactory = mock(PluginFactory.class);
        jsonCodec = new JsonOutputCodec(new JsonOutputCodecConfig());
        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenReturn(jsonCodec);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        codec = mock(PluginModel.class);
        when(codec.getPluginName()).thenReturn("json");
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn("sqs");
        when(pluginSetting.getPipelineName()).thenReturn("test-pipeline");
        when(codec.getPluginSettings()).thenReturn(new HashMap<String, Object>());
        groupId = "testGroupId";
        count = new AtomicInteger(0);
        objectMapper = new ObjectMapper();
        sinkContext = mock(SinkContext.class);
        eventHandle = mock(EventHandle.class);
        when(sinkContext.getExcludeKeys()).thenReturn(null);
        when(sinkContext.getIncludeKeys()).thenReturn(null);
        when(sinkContext.getTagsTargetKey()).thenReturn(null);
        awsRegion = System.getProperty("tests.aws.region");
        awsRole = System.getProperty("tests.aws.role");
        bucket = System.getProperty("tests.s3.bucket");
        awsConfig = mock(AwsConfig.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.of(awsRegion));
        when(awsConfig.getAwsStsRoleArn()).thenReturn(awsRole);
        when(awsConfig.getAwsStsExternalId()).thenReturn(null);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(null);
        when(awsCredentialsSupplier.getProvider(any())).thenAnswer(options -> DefaultCredentialsProvider.create());
        sqsClient = SqsClientFactory.createSqsClient(Region.of(awsRegion), DefaultCredentialsProvider.create());
        queueUrl = System.getProperty("tests.sqs.queue_url");
        sqsSinkConfig = mock(SqsSinkConfig.class);
        when(sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl);
        when(sqsSinkConfig.getGroupId()).thenReturn(groupId);
        when(sqsSinkConfig.getCodec()).thenReturn(codec);
        when(sqsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(sqsSinkConfig.getDlq()).thenReturn(null);

        thresholdConfig = mock(SqsThresholdConfig.class);
        when(sqsSinkConfig.getMaxRetries()).thenReturn(3);
        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        when(thresholdConfig.getMaxMessageSizeBytes()).thenReturn(250*1024L);
        when(sqsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
        try {
            purgeMessages();
        } catch (Exception e){}
    }

    private void purgeMessages() {
        sqsClient.purgeQueue(PurgeQueueRequest.builder()
                .queueUrl(queueUrl)
                .build());
    }

    @AfterEach
    void tearDown() {
        List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
        int i = 0;
        for (final Message message : messages) {
            entries.add(DeleteMessageBatchRequestEntry.builder()
                .id(message.messageId())
                .receiptHandle(message.receiptHandle())
                .build());

            if (++i == 10) {
                DeleteMessageBatchResponse response =
                    sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build());
                i = 0;
                entries.clear();
            }
        }
        if (i > 0) {
            DeleteMessageBatchResponse response =
                sqsClient.deleteMessageBatch(DeleteMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build());
        }
        deleteObjectsWithPrefix(bucket, DLQ_PREFIX);
    }

    private List<String> listObjectsWithPrefix(String bucketName, String prefix) {
        List<String> objectNames = new ArrayList<>();
        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(prefix).build();

        ListObjectsResponse result = s3Client.listObjects(request);
        for (final S3Object s3Object : result.contents()) {
            objectNames.add(s3Object.key());
        }
        return objectNames;
    }

    private void deleteObjectsWithPrefix(String bucketName, String prefix) {
        if (s3Client != null) {
            List<String> objectNames = listObjectsWithPrefix(bucketName, prefix);
            for (final String objectName : objectNames) {
                final DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                                .bucket(bucket)
                                .key(objectName).build();
                s3Client.deleteObject(deleteObjectRequest);
            }
        }
    }

    private SqsSink createObjectUnderTest() {
        return new SqsSink(pluginSetting, pluginMetrics, pluginFactory, sqsSinkConfig, sinkContext, expressionEvaluator, awsCredentialsSupplier);
    }

    private List<Message> getMessages(final String queueUrl) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(3)
                .attributeNamesWithStrings("All")
                .messageAttributeNames("All")
                .build();
        return sqsClient.receiveMessage(request).messages();
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestSinkOperationWithBatchSize(int numRecords) throws Exception {
        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(numRecords, false);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    final Map<String, Object> expectedMap = new HashMap<>();
                    for (int i = 0; i < numRecords; i++) {
                        expectedMap.put("Person"+i, Integer.toString(i));
                    }
                    List<Message> msgs = getMessages(queueUrl);
                    messages.addAll(msgs);
                    assertThat(messages.size(), equalTo(numRecords));
                    for (int i = 0; i < messages.size(); i++) {
                        String body = messages.get(i).body();
                        Map<String, Object> events = objectMapper.readValue(body, Map.class);
                        List<Object> objs = (List<Object>)events.get("events");
                        assertNotNull(objs);
                        assertThat(objs.size(), equalTo(1));
                        Map<String, Object> event = (Map<String, Object>)objs.get(0);
                        String name = (String)event.get("name");
                        assertTrue(expectedMap.containsKey(name));
                        assertThat(expectedMap.get(name), equalTo(event.get("age")));
                        expectedMap.remove(name);
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50, 70})
    void TestSinkOperationWithBatchSizeWithSinkContext(int numRecords) throws Exception {
        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        when(sinkContext.getTagsTargetKey()).thenReturn("sqsSinkTags");
        when(sinkContext.getExcludeKeys()).thenReturn(List.of("age"));
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(numRecords, false);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    final Map<String, Object> expectedMap = new HashMap<>();
                    for (int i = 0; i < numRecords; i++) {
                        expectedMap.put("Person"+i, Integer.toString(i));
                    }
                    List<Message> msgs = getMessages(queueUrl);
                    messages.addAll(msgs);
                    assertThat(messages.size(), equalTo(numRecords));
                    for (int i = 0; i < messages.size(); i++) {
                        String body = messages.get(i).body();
                        Map<String, Object> events = objectMapper.readValue(body, Map.class);
                        List<Object> objs = (List<Object>)events.get("events");
                        assertNotNull(objs);
                        assertThat(objs.size(), equalTo(1));
                        Map<String, Object> event = (Map<String, Object>)objs.get(0);
                        String name = (String)event.get("name");
                        assertTrue(expectedMap.containsKey(name));
                        assertThat(event.get("age"), equalTo(null));
                        assertThat(event.get("sqsSinkTags"), equalTo(List.of()));
                        expectedMap.remove(name);
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(numRecords/10));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 25, 40, 75})
    void TestSinkOperationWithFlushIntervalOneRequest(int numRecords) throws Exception {
        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(50);
        when(thresholdConfig.getFlushInterval()).thenReturn(3L);

        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(numRecords, false);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    final Map<String, Object> expectedMap = new HashMap<>();
                    for (int i = 0; i < numRecords; i++) {
                        expectedMap.put("Person"+i, Integer.toString(i));
                    }
                    List<Message> msgs = getMessages(queueUrl);
                    messages.addAll(msgs);
                    assertThat(messages.size(), equalTo(1 + numRecords/50));
                    int remainingRecords = numRecords;
                    for (int i = 0; i < messages.size(); i++) {
                        String body = messages.get(i).body();
                        Map<String, Object> events = objectMapper.readValue(body, Map.class);
                        List<Object> objs = (List<Object>)events.get("events");
                        assertNotNull(objs);
                        remainingRecords -= objs.size();
                        for (int j = 0; j < objs.size(); j++) {
                            Map<String, Object> event = (Map<String, Object>)objs.get(j);
                            String name = (String)event.get("name");
                            assertTrue(expectedMap.containsKey(name));
                            assertThat(expectedMap.get(name), equalTo(event.get("age")));
                            expectedMap.remove(name);
                        }
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 25, 40, 75})
    void TestSinkOperationWithFlushIntervalMultipleRequests(int numRecords) throws Exception {
        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(5);
        when(thresholdConfig.getFlushInterval()).thenReturn(5L);

        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(numRecords, false);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    final Map<String, Object> expectedMap = new HashMap<>();
                    for (int i = 0; i < numRecords; i++) {
                        expectedMap.put("Person"+i, Integer.toString(i));
                    }
                    List<Message> msgs = getMessages(queueUrl);
                    messages.addAll(msgs);
                    assertThat((double)messages.size(), equalTo(Math.ceil(numRecords/5)));
                    int remainingRecords = numRecords;
                    for (int i = 0; i < messages.size(); i++) {
                        String body = messages.get(i).body();
                        Map<String, Object> events = objectMapper.readValue(body, Map.class);
                        List<Object> objs = (List<Object>)events.get("events");
                        assertNotNull(objs);
                        assertThat(objs.size(), equalTo(5));
                        remainingRecords -= objs.size();
                        for (int j = 0; j < objs.size(); j++) {
                            Map<String, Object> event = (Map<String, Object>)objs.get(j);
                            String name = (String)event.get("name");
                            assertTrue(expectedMap.containsKey(name));
                            assertThat(expectedMap.get(name), equalTo(event.get("age")));
                            expectedMap.remove(name);
                        }
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(numRecords));
        assertThat((double)requestsSuccessCount.get(), equalTo(1.0+numRecords/50));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        verify(eventHandle, times(numRecords)).release(true);
    }

    @Test
    void TestWithLargeSingleMessagesSentToDLQ() throws Exception {
        s3Client = S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(awsRegion))
                .build();
        PluginModel dlqConfig = mock(PluginModel.class);
        when(dlqConfig.getPluginSettings()).thenReturn(new HashMap<String, Object>());
        when(dlqConfig.getPluginName()).thenReturn("s3");

        S3DlqWriterConfig s3DlqWriterConfig = mock(S3DlqWriterConfig.class);
        when(s3DlqWriterConfig.getBucket()).thenReturn(bucket);
        when(s3DlqWriterConfig.getKeyPathPrefix()).thenReturn(DLQ_PREFIX);
        when(s3DlqWriterConfig.getS3Client()).thenReturn(s3Client);
        S3DlqProvider s3DlqProvider = new S3DlqProvider(s3DlqWriterConfig);
        when(pluginFactory.loadPlugin(eq(DlqProvider.class), any())).thenReturn(s3DlqProvider);

        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        when(sqsSinkConfig.getDlq()).thenReturn(dlqConfig);

        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS, false);
        Record<Event> largeRecord = getLargeRecord(256*1024);
        records.add(largeRecord);

        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    final Map<String, Object> expectedMap = new HashMap<>();
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        expectedMap.put("Person"+i, Integer.toString(i));
                    }
                    List<Message> msgs = getMessages(queueUrl);
                    messages.addAll(msgs);
                    assertThat(messages.size(), equalTo(NUM_RECORDS));
                    for (int i = 0; i < messages.size(); i++) {
                        String body = messages.get(i).body();
                        Map<String, Object> events = objectMapper.readValue(body, Map.class);
                        List<Object> objs = (List<Object>)events.get("events");
                        assertNotNull(objs);
                        assertThat(objs.size(), equalTo(1));
                        Map<String, Object> event = (Map<String, Object>)objs.get(0);
                        String name = (String)event.get("name");
                        assertTrue(expectedMap.containsKey(name));
                        assertThat(expectedMap.get(name), equalTo(event.get("age")));
                        expectedMap.remove(name);
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(1));
        verify(eventHandle, times(NUM_RECORDS+1)).release(true);
    }

    private Record<Event> getLargeRecord(int size) {
        final Event event = JacksonLog.builder()
                            .withData(Map.of("key", RandomStringUtils.randomAlphabetic(size)))
                            .withEventHandle(eventHandle)
                            .build();
        return new Record<>(event);
    }


    @Test
    void TestSinkOperationWithQueuesAsExpression() throws Exception {
        s3Client = S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(awsRegion))
                .build();
        PluginModel dlqConfig = mock(PluginModel.class);
        when(dlqConfig.getPluginSettings()).thenReturn(new HashMap<String, Object>());
        when(dlqConfig.getPluginName()).thenReturn("s3");

        S3DlqWriterConfig s3DlqWriterConfig = mock(S3DlqWriterConfig.class);
        when(s3DlqWriterConfig.getBucket()).thenReturn(bucket);
        when(s3DlqWriterConfig.getKeyPathPrefix()).thenReturn(DLQ_PREFIX);
        when(s3DlqWriterConfig.getS3Client()).thenReturn(s3Client);
        S3DlqProvider s3DlqProvider = new S3DlqProvider(s3DlqWriterConfig);
        when(pluginFactory.loadPlugin(eq(DlqProvider.class), any())).thenReturn(s3DlqProvider);

        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        when(sqsSinkConfig.getDlq()).thenReturn(dlqConfig);
        when(expressionEvaluator.isValidFormatExpression(anyString())).thenReturn(true);

        when(sqsSinkConfig.getQueueUrl()).thenReturn(queueUrl+"${/id}");

        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(2*NUM_RECORDS, false);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    final Map<String, Object> expectedMap = new HashMap<>();
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        expectedMap.put("Person"+i, Integer.toString(i));
                    }
                    List<Message> msgs = getMessages(queueUrl);
                    messages.addAll(msgs);
                    assertThat(messages.size(), equalTo(NUM_RECORDS));
                    for (int i = 0; i < messages.size(); i++) {
                        String body = messages.get(i).body();
                        Map<String, Object> events = objectMapper.readValue(body, Map.class);
                        List<Object> objs = (List<Object>)events.get("events");
                        assertNotNull(objs);
                        assertThat(objs.size(), equalTo(1));
                        Map<String, Object> event = (Map<String, Object>)objs.get(0);
                        String name = (String)event.get("name");
                        assertTrue(expectedMap.containsKey(name));
                        assertThat(expectedMap.get(name), equalTo(event.get("age")));
                        expectedMap.remove(name);
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(NUM_RECORDS));
        verify(eventHandle, times(2*NUM_RECORDS)).release(true);
    }

    @RepeatedTest(value = 5)
    void TestWithManyRecordsWithRandomSizes() throws Exception {
        final int numRecords = 100 + random.nextInt(100);
        s3Client = S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(awsRegion))
                .build();
        PluginModel dlqConfig = mock(PluginModel.class);
        when(dlqConfig.getPluginSettings()).thenReturn(new HashMap<String, Object>());
        when(dlqConfig.getPluginName()).thenReturn("s3");
        when(thresholdConfig.getFlushInterval()).thenReturn(3L);

        S3DlqWriterConfig s3DlqWriterConfig = mock(S3DlqWriterConfig.class);
        when(s3DlqWriterConfig.getBucket()).thenReturn(bucket);
        when(s3DlqWriterConfig.getKeyPathPrefix()).thenReturn(DLQ_PREFIX);
        when(s3DlqWriterConfig.getS3Client()).thenReturn(s3Client);
        S3DlqProvider s3DlqProvider = new S3DlqProvider(s3DlqWriterConfig);
        when(pluginFactory.loadPlugin(eq(DlqProvider.class), any())).thenReturn(s3DlqProvider);

        when(thresholdConfig.getMaxEventsPerMessage()).thenReturn(20+random.nextInt(30));
        when(sqsSinkConfig.getDlq()).thenReturn(dlqConfig);

        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(numRecords, true);

        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    final Map<String, Object> expectedMap = new HashMap<>();
                    for (int i = 0; i < numRecords; i++) {
                        expectedMap.put("Person"+i, Integer.toString(i));
                    }
                    List<Message> msgs = getMessages(queueUrl);
                    messages.addAll(msgs);
                    int recordsReceived = 0;
                    for (int i = 0; i < messages.size(); i++) {
                        String body = messages.get(i).body();
                        Map<String, Object> events = objectMapper.readValue(body, Map.class);
                        List<Object> objs = (List<Object>)events.get("events");
                        assertNotNull(objs);
                        recordsReceived += objs.size();
                        for (int j = 0; j < objs.size(); j++) {
                            Map<String, Object> event = (Map<String, Object>)objs.get(j);
                            String name = (String)event.get("name");
                            assertTrue(expectedMap.containsKey(name));
                            expectedMap.remove(name);
                        }
                    }

                    assertThat(recordsReceived, equalTo(numRecords));
                });
        assertThat(eventsSuccessCount.get(), equalTo(numRecords - numLargeMessages));
        assertThat(dlqSuccessCount.get(), equalTo(numLargeMessages));
        verify(eventHandle, times(numRecords)).release(true);
    }


    private Collection<Record<Event>> getRecordList(int numberOfRecords, boolean randomSize) throws Exception {
        final Collection<Record<Event>> recordList = new ArrayList<>();
        List<HashMap> records = generateRecords(numberOfRecords, randomSize);
        for (int i = 0; i < numberOfRecords; i++) {
            final Event event = JacksonLog.builder()
                                .withData(records.get(i))
                                .withEventHandle(eventHandle)
                                .build();
            if (randomSize) {
                long size = jsonCodec.getEstimatedSize(event, new OutputCodecContext());
                if (size > 256*1024) {
                    numLargeMessages++;
                }
            }
            recordList.add(new Record<>(event));
        }
        return recordList;
    }

    private List<HashMap> generateRecords(int numberOfRecords, boolean randomSize) {
        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {
            HashMap<String, String> eventData = new HashMap<>();
            eventData.put("name", "Person" + rows);
            if (!randomSize) {
                eventData.put("age", Integer.toString(rows));
            } else {
                int size = random.nextInt(MAX_SIZE);
                String ageValue = RandomStringUtils.randomAlphabetic(size);
                eventData.put("age", ageValue);
            }
            eventData.put("id", (rows < NUM_RECORDS) ? "": "10");
            recordList.add(eventData);

        }
        return recordList;
    }
}
