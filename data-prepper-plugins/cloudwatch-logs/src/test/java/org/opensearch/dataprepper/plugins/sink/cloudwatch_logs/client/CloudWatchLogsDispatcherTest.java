/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudWatchLogsDispatcherTest {
    private CloudWatchLogsDispatcher cloudWatchLogsDispatcher;
    private  CloudWatchLogsClient mockCloudWatchLogsClient;
    private CloudWatchLogsMetrics mockCloudWatchLogsMetrics;
    private Executor mockExecutor;
    private static final int RETRY_COUNT = 5;
    private static final String LOG_GROUP = "testGroup";
    private static final String LOG_STREAM = "testStream";
    private static final String TEST_STRING = "testMessage";

    @BeforeEach
    void setUp() {
        mockCloudWatchLogsClient = mock(CloudWatchLogsClient.class);
        mockCloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);
        mockExecutor = mock(Executor.class);
    }

    Collection<byte[]> getSampleBufferedData() {
        final ArrayList<byte[]> returnCollection = new ArrayList<>();

        for (int i = 0; i < ThresholdConfig.DEFAULT_BATCH_SIZE; i++) {
            returnCollection.add(new String(TEST_STRING).getBytes());
        }

        return returnCollection;
    }

    List<EventHandle> getSampleEventHandles() {
        final ArrayList<EventHandle> eventHandles = new ArrayList<>();

        for (int i = 0; i < ThresholdConfig.DEFAULT_BATCH_SIZE; i++) {
            final EventHandle mockEventHandle = mock(EventHandle.class);
            eventHandles.add(mockEventHandle);
        }

        return eventHandles;
    }

    CloudWatchLogsDispatcher getCloudWatchLogsDispatcher() {
        return CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(mockCloudWatchLogsClient)
                .cloudWatchLogsMetrics(mockCloudWatchLogsMetrics)
                .executor(mockExecutor)
                .logGroup(LOG_GROUP)
                .logStream(LOG_STREAM)
                .retryCount(RETRY_COUNT)
                .build();
    }

    CloudWatchLogsDispatcher getCloudWatchLogsDispatcherWithEntity(EntityConfig entity) {
        return CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(mockCloudWatchLogsClient)
                .cloudWatchLogsMetrics(mockCloudWatchLogsMetrics)
                .executor(mockExecutor)
                .logGroup(LOG_GROUP)
                .logStream(LOG_STREAM)
                .retryCount(RETRY_COUNT)
                .entity(entity)
                .build();
    }

    @Test
    void GIVEN_valid_input_log_events_SHOULD_call_executor() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcher();
        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, getSampleEventHandles());

        verify(mockExecutor, atMostOnce()).execute(any(CloudWatchLogsDispatcher.Uploader.class));
    }

    @Test
    void GIVEN_valid_input_log_events_with_entity_SHOULD_call_executor() {
        EntityConfig entity = mock(EntityConfig.class);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("attr1", "value1");
        Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("key1", "value1");
        
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithEntity(entity);
        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, getSampleEventHandles());

        verify(mockExecutor, atMostOnce()).execute(any(CloudWatchLogsDispatcher.Uploader.class));
    }

    @Test
    void GIVEN_null_entity_SHOULD_call_executor() {
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithEntity(null);
        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, getSampleEventHandles());

        verify(mockExecutor, atMostOnce()).execute(any(CloudWatchLogsDispatcher.Uploader.class));
    }

    @Test
    void GIVEN_entity_with_null_attributes_SHOULD_call_executor() {
        EntityConfig entity = mock(EntityConfig.class);
        when(entity.getAttributes()).thenReturn(null);
        when(entity.getKeyAttributes()).thenReturn(null);
        
        cloudWatchLogsDispatcher = getCloudWatchLogsDispatcherWithEntity(entity);
        List<InputLogEvent> inputLogEventList = cloudWatchLogsDispatcher.prepareInputLogEvents(getSampleBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEventList, getSampleEventHandles());

        verify(mockExecutor, atMostOnce()).execute(any(CloudWatchLogsDispatcher.Uploader.class));
    }
}
