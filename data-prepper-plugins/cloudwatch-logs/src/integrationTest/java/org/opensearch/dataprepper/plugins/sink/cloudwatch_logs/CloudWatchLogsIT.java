/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

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
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsClientFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamResponse;
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
    private CloudWatchLogsClient cloudWatchLogsClient;
    private ObjectMapper objectMapper;
    private AwsCredentialsProvider awsCredentialsProvider;
    private S3Client s3Client;

    @BeforeEach
    void setUp() {
        awsCredentialsProvider = DefaultCredentialsProvider.create();
        eventsSuccessCount = new AtomicInteger(0);
        requestsSuccessCount = new AtomicInteger(0);
        eventsFailedCount = new AtomicInteger(0);
        requestsFailedCount = new AtomicInteger(0);
        dlqSuccessCount = new AtomicInteger(0);
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
        cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);
        logGroupName = System.getProperty("tests.cloudwatch.log_group");
        logStreamName = createLogStream(logGroupName);
        pluginMetrics = mock(PluginMetrics.class);
        eventsSuccessCounter = mock(Counter.class);
        requestsSuccessCounter = mock(Counter.class);
        eventsFailedCounter = mock(Counter.class);
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
            if (s.equals(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED)) {
                return requestsSuccessCounter;
            }
            if (s.equals(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SUCCEEDED)) {
                return eventsSuccessCounter;
            }
            if (s.equals(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED)) {
                return requestsFailedCounter;
            }
            if (s.equals(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_FAILED)) {
                return eventsFailedCounter;
            }
            if (s.contains("NumDlqSuccess")) {
                return dlqSuccessCounter;
            }
            return null;
        }).when(pluginMetrics).counter(anyString());
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn(logGroupName);
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(null);
        when(cloudWatchLogsSinkConfig.getLogStream()).thenReturn(logStreamName);
        when(cloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(cloudWatchLogsSinkConfig.getBufferType()).thenReturn(CloudWatchLogsSinkConfig.DEFAULT_BUFFER_TYPE);

        thresholdConfig = mock(ThresholdConfig.class);
        when(thresholdConfig.getBackOffTime()).thenReturn(500L);
        when(thresholdConfig.getLogSendInterval()).thenReturn(60L);
        when(thresholdConfig.getRetryCount()).thenReturn(10);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
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

    @AfterEach
    void tearDown() {
        DeleteLogStreamRequest deleteRequest = DeleteLogStreamRequest
                                        .builder()
                                        .logGroupName(logGroupName)
                                        .logStreamName(logStreamName)
                                        .build();
        cloudWatchLogsClient.deleteLogStream(deleteRequest);
        deleteObjectsWithPrefix(bucket, DLQ_PREFIX);
    }

    private CloudWatchLogsSink createObjectUnderTest() {
        return new CloudWatchLogsSink(pluginSetting, pluginMetrics, pluginFactory, cloudWatchLogsSinkConfig, awsCredentialsSupplier);
    }
    
    private String createLogStream(final String logGroupName) {
        final String newLogStreamName = "CouldWatchLogsIT_"+RandomStringUtils.randomAlphabetic(6);
        CreateLogStreamRequest createRequest = CreateLogStreamRequest
                                         .builder()
                                         .logGroupName(logGroupName)
                                         .logStreamName(newLogStreamName)
                                         .build();
        CreateLogStreamResponse response = cloudWatchLogsClient.createLogStream(createRequest);
        return newLogStreamName;
        
    }

    @Test
    void TestSinkOperationWithLogSendInterval() throws Exception {
        long startTime = Instant.now().toEpochMilli();
        when(thresholdConfig.getBatchSize()).thenReturn(10);
        when(thresholdConfig.getLogSendInterval()).thenReturn(10L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(null);
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS);
        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    long endTime = Instant.now().toEpochMilli();
                    GetLogEventsRequest getRequest = GetLogEventsRequest
                                       .builder()
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
                        assertThat(event.get("name"), equalTo("Person"+i));
                        assertThat(event.get("age"), equalTo(Integer.toString(i)));
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(0));

    }

    @Test
    void TestSinkOperationWithBatchSize() throws Exception {
        long startTime = Instant.now().toEpochMilli();
        when(thresholdConfig.getBatchSize()).thenReturn(1);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(1000L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS);
        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    long endTime = Instant.now().toEpochMilli();
                    GetLogEventsRequest getRequest = GetLogEventsRequest
                                       .builder()
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
                        assertThat(event.get("name"), equalTo("Person"+i));
                        assertThat(event.get("age"), equalTo(Integer.toString(i)));
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(dlqSuccessCount.get(), equalTo(0));

    }

    @Test
    void TestSinkOperationWithMaxRequestSize() throws Exception {
        long startTime = Instant.now().toEpochMilli();
        when(thresholdConfig.getBatchSize()).thenReturn(20);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(108L);
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS+1);
        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    long endTime = Instant.now().toEpochMilli();
                    GetLogEventsRequest getRequest = GetLogEventsRequest
                                       .builder()
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
                        assertThat(event.get("name"), equalTo("Person"+i));
                        assertThat(event.get("age"), equalTo(Integer.toString(i)));
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(0));

    }

    @Test
    void testWithLargeSingleMessagesSentToDLQ() {
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
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(s3DlqProvider);

        long startTime = Instant.now().toEpochMilli();
        when(thresholdConfig.getBatchSize()).thenReturn(NUM_RECORDS);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(200L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(dlqConfig);
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS);
        Record<Event> largeRecord = getLargeRecord(200);
        records.add(largeRecord);

        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    long endTime = Instant.now().toEpochMilli();
                    GetLogEventsRequest getRequest = GetLogEventsRequest
                                       .builder()
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
                        assertThat(event.get("name"), equalTo("Person"+i));
                        assertThat(event.get("age"), equalTo(Integer.toString(i)));
                    }
                });
        assertThat(eventsSuccessCount.get(), equalTo(NUM_RECORDS));
        assertThat(requestsSuccessCount.get(), equalTo(1));
        assertThat(dlqSuccessCount.get(), equalTo(1));

    }

    @Test
    void testWithBadCredentials_AllEventsToDLQ() {
        s3Client = S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.of(awsRegion))
                .build();
        PluginModel dlqConfig = mock(PluginModel.class);
        when(dlqConfig.getPluginSettings()).thenReturn(new HashMap<String, Object>());
        when(dlqConfig.getPluginName()).thenReturn("s3");

        S3DlqWriterConfig s3DlqWriterConfig = mock(S3DlqWriterConfig.class);
        when(s3DlqWriterConfig.getBucket()).thenReturn(bucket);
        when(s3DlqWriterConfig.getKeyPathPrefix()).thenReturn("cloudWatchLogsIT/");
        when(s3DlqWriterConfig.getS3Client()).thenReturn(s3Client);
        S3DlqProvider s3DlqProvider = new S3DlqProvider(s3DlqWriterConfig);
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(s3DlqProvider);

        long startTime = Instant.now().toEpochMilli();
        when(thresholdConfig.getBatchSize()).thenReturn(NUM_RECORDS);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(200L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        when(cloudWatchLogsSinkConfig.getDlq()).thenReturn(dlqConfig);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn("dummyLogGroup");
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS);

        sink.doOutput(records);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    assertThat(dlqSuccessCount.get(), equalTo(NUM_RECORDS));
                });
        assertThat(eventsSuccessCount.get(), equalTo(0));
        assertThat(requestsSuccessCount.get(), equalTo(0));
        assertThat(dlqSuccessCount.get(), equalTo(NUM_RECORDS));

    }

    private Collection<Record<Event>> getRecordList(int numberOfRecords) {
        final Collection<Record<Event>> recordList = new ArrayList<>();
        List<HashMap> records = generateRecords(numberOfRecords);
        for (int i = 0; i < numberOfRecords; i++) {
            final Event event = JacksonLog.builder().withData(records.get(i)).build();
            recordList.add(new Record<>(event));
        }
        return recordList;
    }

    private Record<Event> getLargeRecord(int size) {
        final Event event = JacksonLog.builder().withData(Map.of("key", RandomStringUtils.randomAlphabetic(size))).build();
        return new Record<>(event);
    }


    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, String> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add((eventData));

        }
        return recordList;
    }
}
