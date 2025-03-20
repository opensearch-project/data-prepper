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
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsClientFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
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
public class CouldWatchLogsIT {
    static final int NUM_RECORDS = 2;
    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsConfig awsConfig;

    @Mock
    private ThresholdConfig thresholdConfig;

    @Mock
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;

    @Mock
    private Counter counter;

    private String awsRegion;
    private String awsRole;
    private String logGroupName;
    private String logStreamName;
    private CloudWatchLogsSink sink;
    private AtomicInteger count;
    private CloudWatchLogsClient cloudWatchLogsClient;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        count = new AtomicInteger(0);
        objectMapper = new ObjectMapper();
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(pluginSetting.getName()).thenReturn("name");
        awsRegion = System.getProperty("tests.aws.region");
        awsRole = System.getProperty("tests.aws.role");
        awsConfig = mock(AwsConfig.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.of(awsRegion));
        when(awsConfig.getAwsStsRoleArn()).thenReturn(awsRole);
        when(awsConfig.getAwsStsExternalId()).thenReturn(null);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(null);
        when(awsCredentialsSupplier.getProvider(any())).thenAnswer(options -> DefaultCredentialsProvider.create());
        cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);
        logGroupName = System.getProperty("tests.cloudwatch.log_group");
        logStreamName = createLogStream(logGroupName);
        pluginMetrics = mock(PluginMetrics.class);
        counter = mock(Counter.class);
        lenient().doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            count.addAndGet(v);
            return null;
        }).when(counter).increment(any(Double.class));
        lenient().doAnswer((a)-> {
            count.addAndGet(1);
            return null;
        }).when(counter).increment();
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        cloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        when(cloudWatchLogsSinkConfig.getLogGroup()).thenReturn(logGroupName);
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

    @AfterEach
    void tearDown() {
        DeleteLogStreamRequest deleteRequest = DeleteLogStreamRequest
                                        .builder()
                                        .logGroupName(logGroupName)
                                        .logStreamName(logStreamName)
                                        .build();
        cloudWatchLogsClient.deleteLogStream(deleteRequest);
    }

    private CloudWatchLogsSink createObjectUnderTest() {
        return new CloudWatchLogsSink(pluginSetting, pluginMetrics, cloudWatchLogsSinkConfig, awsCredentialsSupplier);
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
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS);
        sink.doOutput(records, null);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList(), null);
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
        // NUM_RECORDS success
        // 1 request success
        assertThat(count.get(), equalTo(NUM_RECORDS+1));

    }

    @Test
    void TestSinkOperationWithBatchSize() throws Exception {
        long startTime = Instant.now().toEpochMilli();
        when(thresholdConfig.getBatchSize()).thenReturn(1);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(1000L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(1000L);
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS);
        sink.doOutput(records, null);
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
        // NUM_RECORDS success
        // NUM_RECORDS request success
        assertThat(count.get(), equalTo(NUM_RECORDS*2));

    }

    @Test
    void TestSinkOperationWithMaxRequestSize() throws Exception {
        long startTime = Instant.now().toEpochMilli();
        when(thresholdConfig.getBatchSize()).thenReturn(20);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(108L);
        
        sink = createObjectUnderTest();
        Collection<Record<Event>> records = getRecordList(NUM_RECORDS);
        sink.doOutput(records, null);
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
        // NUM_RECORDS success
        // 1 request success
        assertThat(count.get(), equalTo(NUM_RECORDS+1));

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
