/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.Counter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import org.opensearch.dataprepper.plugins.dlq.s3.S3DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.s3.S3DlqWriterConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsClientFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.regions.Region;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ExtendWith(MockitoExtension.class)
public class CloudWatchLogsIT {
    static final int NUM_RECORDS = 2;
    static final String DLQ_PREFIX = "cloudWatchLogsIT/";

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsConfig awsConfig;
    @Mock
    private ThresholdConfig thresholdConfig;
    @Mock
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;

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
    private Counter dlqFailedCounter;
    @Mock
    private Counter entityRejectedCounter;

    private String awsRegion;
    private String awsRole;
    private String bucket;
    private String logGroupName;
    private String logStreamName;
    private CloudWatchLogsSink sink;
    private AtomicInteger eventsSuccessCount;
    private AtomicInteger requestsSuccessCount;
    private AtomicInteger eventsFailedCount;
    private AtomicInteger requestsFailedCount;
    private AtomicInteger dlqSuccessCount;
    private AtomicInteger dlqFailedCount;
    private AtomicInteger entityRejectedCount;
    private CloudWatchLogsClient cloudWatchLogsClient;
    private ObjectMapper objectMapper;
    private AwsCredentialsProvider awsCredentialsProvider;
    private S3Client s3Client;
    private long startTime;

    @BeforeEach
    void setUp() {
        awsCredentialsProvider = DefaultCredentialsProvider.create();
        eventsSuccessCount = new AtomicInteger(0);
        requestsSuccessCount = new AtomicInteger(0);
        eventsFailedCount = new AtomicInteger(0);
        requestsFailedCount = new AtomicInteger(0);
        dlqSuccessCount = new AtomicInteger(0);
        entityRejectedCount = new AtomicInteger(0);
        objectMapper = new ObjectMapper();
        pluginSetting = mock(PluginSetting.class);
        pluginFactory = mock(PluginFactory.class);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(pluginSetting.getName()).thenReturn("name");
        awsRegion = System.getProperty("tests.aws.region");
        awsRole = System.getProperty("tests.aws.role");
        bucket = System.getProperty("tests.s3.bucket");
        awsConfig = mock(AwsConfig.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.of(awsRegion));
        when(awsConfig.getAwsStsRoleArn()).thenReturn(awsRole);
        when(awsConfig.getAwsStsExternalId()).thenReturn(null);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(null);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier, new HashMap<>(), null);
        logGroupName = System.getProperty("tests.cloudwatch.log_group");
        logStreamName = createLogStream(logGroupName);
        pluginMetrics = mock(PluginMetrics.class);
        eventsSuccessCounter = mock(Counter.class);
        requestsSuccessCounter = mock(Counter.class);
        eventsFailedCounter = mock(Counter.class);
        requestsFailedCounter = mock(Counter.class);
        dlqSuccessCounter = mock(Counter.class);
        lenient().doAnswer((a) -> {
            int v = (int) (double) (a.getArgument(0));
            eventsSuccessCount.addAndGet(v);
            return null;
        }).when(eventsSuccessCounter).increment(any(Double.class));
        lenient().doAnswer((a) -> {
            int v = (int) (double) (a.getArgument(0));
            eventsFailedCount.addAndGet(v);
            return null;
        }).when(eventsFailedCounter).increment(any(Double.class));
        lenient().doAnswer((a) -> {
            requestsSuccessCount.addAndGet(1);
            return null;
        }).when(requestsSuccessCounter).increment();
        lenient().doAnswer((a) -> {
            int v = (int) (double) (a.getArgument(0));
            requestsSuccessCount.addAndGet(v);
            return null;
        }).when(requestsSuccessCounter).increment(any(Double.class));
        lenient().doAnswer((a) -> {
            int v = (int) (double) (a.getArgument(0));
            requestsFailedCount.addAndGet(v);
            return null;
        }).when(requestsFailedCounter).increment(any(Double.class));
        lenient().doAnswer((a) -> {
            int v = (int) (double) (a.getArgument(0));
            dlqSuccessCount.addAndGet(v);
            return null;
        }).when(dlqSuccessCounter).increment(any(Double.class));
        lenient().doAnswer((a) -> {
            int v = (int) (double) (a.getArgument(0));
            dlqFailedCount.addAndGet(v);
            return null;
        }).when(dlqFailedCounter).increment(any(Double.class));
        lenient().doAnswer((a) -> {
            entityRejectedCount.addAndGet(1);
            return null;
        }).when(entityRejectedCounter).increment();
        lenient().doAnswer(a -> {
            String s = a.getArgument(0);
            switch (s) {
            case CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED:
                return requestsSuccessCounter;
            case CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SUCCEEDED:
                return eventsSuccessCounter;
            case CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED:
                return requestsFailedCounter;
            case CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_FAILED:
                return eventsFailedCounter;
            case CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_REJECTED:
                return entityRejectedCounter;
            case "cloudWatchLogsNumDlqSuccess":
                return dlqSuccessCounter;
            case "cloudWatchLogsNumDlqFailed":
                return dlqFailedCounter;
            default:
                return null;
            }
        }).when(pluginMetrics).counter(anyString());
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn(logGroupName);
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(null);
        when(cloudWatchLogsSinkConfig.getLogStream()).thenReturn(logStreamName);
        when(cloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cloudWatchLogsSinkConfig.getMaxRetries()).thenReturn(3);
        when(cloudWatchLogsSinkConfig.getWorkers()).thenReturn(1);
        when(cloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(new HashMap<>());

        thresholdConfig = mock(ThresholdConfig.class);
        when(thresholdConfig.getFlushInterval()).thenReturn(60L);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
        
        startTime = Instant.now().toEpochMilli();
    }

    @AfterEach
    void tearDown() {
        DeleteLogStreamRequest deleteRequest = DeleteLogStreamRequest
                .builder()
                .logGroupName(logGroupName)
                .logStreamName(logStreamName)
                .build();
        cloudWatchLogsClient.deleteLogStream(deleteRequest);
        deleteObjectsWithPrefix(bucket);
    }

    @Test
    void testSinkOperationWithLogSendInterval() {
        when(thresholdConfig.getBatchSize()).thenReturn(10);
        when(thresholdConfig.getFlushInterval()).thenReturn(10L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(null);
        mockValidEntity();

        sink = createSink();
        Collection<Record<Event>> records = createRecords(NUM_RECORDS);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    verifyGetLogEvents();
                });
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        assertThat(entityRejectedCount.get(), equalTo(0));
    }

    @Test
    void testSinkOperationWithBatchSize() {
        when(thresholdConfig.getBatchSize()).thenReturn(1);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(1000L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        mockValidEntity();

        sink = createSink();
        Collection<Record<Event>> records = createRecords(NUM_RECORDS);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(this::verifyGetLogEvents);
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        assertThat(entityRejectedCount.get(), equalTo(0));
    }

    @Test
    void testSinkOperationWithMaxRequestSize() {
        when(thresholdConfig.getBatchSize()).thenReturn(20);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(108L);
        mockValidEntity();

        sink = createSink();
        Collection<Record<Event>> records = createRecords(NUM_RECORDS + 1);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(this::verifyGetLogEvents);
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        assertThat(entityRejectedCount.get(), equalTo(0));
    }

    @Test
    void testWithLargeSingleMessagesSentToDLQ() {
        s3Client = createS3Client();
        when(thresholdConfig.getBatchSize()).thenReturn(NUM_RECORDS);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(200L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        mockDlqConfig();
        mockValidEntity();

        sink = createSink();
        Collection<Record<Event>> records = createRecords(NUM_RECORDS);
        Record<Event> largeRecord = createLargeRecord();
        records.add(largeRecord);

        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(this::verifyGetLogEvents);
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(1));
        assertThat(entityRejectedCount.get(), equalTo(0));
    }

    @Test
    void testWithBadCredentials_AllEventsToDLQ() {
        s3Client = createS3Client();
        when(thresholdConfig.getBatchSize()).thenReturn(NUM_RECORDS);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(200L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn("dummyLogGroup");
        mockDlqConfig();
        mockValidEntity();

        sink = createSink();
        Collection<Record<Event>> records = createRecords(NUM_RECORDS);

        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(dlqSuccessCount.get(), equalTo(NUM_RECORDS)));
        assertThat(eventsSuccessCount.get(), equalTo(0));
        assertThat(requestsSuccessCount.get(), equalTo(0));
        assertThat(dlqSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(entityRejectedCount.get(), equalTo(0));
    }

    @Test
    void testWithInvalidEntity_ShouldSucceedAndNotifyEntityRejected() {
        when(thresholdConfig.getBatchSize()).thenReturn(NUM_RECORDS);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(1000L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(null);
        mockInvalidEntity();

        sink = createSink();
        Collection<Record<Event>> records = createRecords(NUM_RECORDS);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(this::verifyGetLogEvents);

        // Events should still succeed even with invalid entity
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(0));
        assertThat(entityRejectedCount.get(), equalTo(1));
    }

    private String createLogStream(final String logGroupName) {
        final String newLogStreamName = "CouldWatchLogsIT_" + RandomStringUtils.randomAlphabetic(6);
        CreateLogStreamRequest createRequest = CreateLogStreamRequest
                .builder()
                .logGroupName(logGroupName)
                .logStreamName(newLogStreamName)
                .build();

        cloudWatchLogsClient.createLogStream(createRequest);

        return newLogStreamName;
    }

    private S3Client createS3Client() {
        return S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(awsRegion))
                .build();
    }

    private void mockDlqConfig() {
        PluginModel dlqConfig = mock(PluginModel.class);
        when(dlqConfig.getPluginSettings()).thenReturn(new HashMap<>());
        when(dlqConfig.getPluginName()).thenReturn("s3");
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(dlqConfig);

        S3DlqWriterConfig s3DlqWriterConfig = mock(S3DlqWriterConfig.class);
        when(s3DlqWriterConfig.getBucket()).thenReturn(bucket);
        when(s3DlqWriterConfig.getKeyPathPrefix()).thenReturn(DLQ_PREFIX);
        when(s3DlqWriterConfig.getS3Client()).thenReturn(s3Client);
        S3DlqProvider s3DlqProvider = new S3DlqProvider(s3DlqWriterConfig);
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(s3DlqProvider);
    }

    private void mockValidEntity() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("PlatformType", "Generic");
        attributes.put("Host", "my.host.com");
        Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "cloudwatch-sink");
        keyAttributes.put("Environment", "integration-tests");

        mockEntity(attributes, keyAttributes);
    }

    private void mockInvalidEntity() {
        Map<String, String> attributes = new HashMap<>();
        // Invalid: attribute names with spaces and special characters
        attributes.put("invalid attribute!", "test-service");
        attributes.put("service@version", "1.0.0");
        Map<String, String> keyAttributes = new HashMap<>();
        // Invalid: key attribute names that don't follow AWS conventions
        keyAttributes.put("invalid key!", "integration-test");
        keyAttributes.put("environment space", "test");

        mockEntity(attributes, keyAttributes);
    }

    private void mockEntity(Map<String, String> attributes, Map<String, String> keyAttributes) {
        EntityConfig entityConfig = mock(EntityConfig.class);
        when(entityConfig.getAttributes()).thenReturn(attributes);
        when(entityConfig.getKeyAttributes()).thenReturn(keyAttributes);

        when(cloudWatchLogsSinkConfig.getEntity()).thenReturn(entityConfig);
    }

    private CloudWatchLogsSink createSink() {
        return new CloudWatchLogsSink(pluginSetting, pluginMetrics, pluginFactory, cloudWatchLogsSinkConfig,
                awsCredentialsSupplier);
    }

    private Collection<Record<Event>> createRecords(int numberOfRecords) {
        return createLogData(numberOfRecords)
                .stream()
                .map(data -> new Record<Event>(JacksonLog.builder()
                        .withData(data)
                        .build()))
                .collect(Collectors.toList());
    }

    private Record<Event> createLargeRecord() {
        final Event event = JacksonLog.builder()
                .withData(Map.of("key", RandomStringUtils.randomAlphabetic(200)))
                .build();

        return new Record<>(event);
    }

    private static List<HashMap<String, String>> createLogData(int numberOfRecords) {
        return IntStream.range(0, numberOfRecords)
                .mapToObj(rows -> {
                    HashMap<String, String> eventData = new HashMap<>();
                    eventData.put("name", "Person" + rows);
                    eventData.put("age", Integer.toString(rows));
                    return eventData;
                })
                .collect(Collectors.toList());
    }

    private List<String> listObjectsWithPrefix(String bucketName) {
        List<String> objectNames = new ArrayList<>();
        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(DLQ_PREFIX).build();

        ListObjectsResponse result = s3Client.listObjects(request);
        for (final S3Object s3Object : result.contents()) {
            objectNames.add(s3Object.key());
        }
        return objectNames;
    }

    private void deleteObjectsWithPrefix(String bucketName) {
        if (s3Client != null) {
            List<String> objectNames = listObjectsWithPrefix(bucketName);
            for (final String objectName : objectNames) {
                final DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectName).build();
                s3Client.deleteObject(deleteObjectRequest);
            }
        }
    }

    private void verifyGetLogEvents() throws JsonProcessingException {
        long endTime = Instant.now().toEpochMilli();
        GetLogEventsRequest getRequest = GetLogEventsRequest.builder()
                .logGroupName(logGroupName)
                .logStreamName(logStreamName)
                .startTime(startTime)
                .endTime(endTime)
                .build();
        GetLogEventsResponse response = cloudWatchLogsClient.getLogEvents(getRequest);
        List<OutputLogEvent> events = response.events();

        assertThat(events.size(), equalTo(NUM_RECORDS));
        for (int i = 0; i < events.size(); i++) {
            String message = events.get(i).message();
            Map<String, Object> event = objectMapper.readValue(message, Map.class);
            assertThat(event.get("name"), equalTo("Person" + i));
            assertThat(event.get("age"), equalTo(Integer.toString(i)));
        }
    }
}
