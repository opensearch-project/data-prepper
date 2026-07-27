/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudWatchLogsServiceTest {
    private static final int LARGE_THREAD_COUNT = 1000;
    private CloudWatchLogsClient mockClient;
    private CloudWatchLogsMetrics mockMetrics;
    private CloudWatchLogsService cloudWatchLogsService;
    private CloudWatchLogsSinkConfig mockCloudWatchLogsSinkConfig;
    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsLimits cloudWatchLogsLimits;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private InMemoryBufferFactory inMemoryBufferFactory;
    private Buffer buffer;
    private CloudWatchLogsDispatcher mockDispatcher;
    private DlqPushHandler dlqPushHandler;
    private EventHandle eventHandle;

    @BeforeEach
    void setUp() {
        mockCloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);

        eventHandle = mock(EventHandle.class);
        cloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);
        thresholdConfig = new ThresholdConfig();
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(), thresholdConfig.getFlushInterval());

        mockClient = mock(CloudWatchLogsClient.class);
        mockMetrics = mock(CloudWatchLogsMetrics.class);
        inMemoryBufferFactory = new InMemoryBufferFactory();
        mockDispatcher = mock(CloudWatchLogsDispatcher.class);
        dlqPushHandler = mock(DlqPushHandler.class);
        cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsMetrics,
                cloudWatchLogsLimits, mockDispatcher, null, true);
    }

    Collection<Record<Event>> getSampleRecordsCollectionSmall() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsCollection() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            JacksonEvent mockJacksonEvent = (JacksonEvent) JacksonEvent.fromMessage("testMessage");
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsOfLargerSize() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        int messageSize = (int) (thresholdConfig.getMaxRequestSizeBytes() / 24);
        for (int i = 0; i < thresholdConfig.getBatchSize() * 2; i++) {
            JacksonEvent mockJacksonEvent =
                    (JacksonEvent) JacksonEvent.fromMessage(RandomStringUtils.insecure().nextAlphabetic(messageSize));
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    Collection<Record<Event>> getSampleRecordsOfLimitSize() {
        final ArrayList<Record<Event>> returnCollection = new ArrayList<>();
        int messageSize = (int) thresholdConfig.getMaxEventSizeBytes();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            JacksonEvent mockJacksonEvent =
                    (JacksonEvent) JacksonEvent.fromMessage(RandomStringUtils.insecure().nextAlphabetic(messageSize));
            returnCollection.add(new Record<>(mockJacksonEvent));
        }

        return returnCollection;
    }

    void setUpSpyBuffer() {
        buffer = spy(InMemoryBuffer.class);
    }

    void setUpRealBuffer() {
        buffer = inMemoryBufferFactory.getBuffer();
    }

    CloudWatchLogsService getSampleService(DlqPushHandler dlqPushHandler) {
        return new CloudWatchLogsService(buffer, cloudWatchLogsMetrics, cloudWatchLogsLimits, mockDispatcher, dlqPushHandler, true);
    }

    CloudWatchLogsService getSampleService() {
        return new CloudWatchLogsService(buffer, cloudWatchLogsMetrics, cloudWatchLogsLimits, mockDispatcher, null, true);
    }

    @Test
    void SHOULD_not_call_dispatcher_WHEN_process_log_events_called_with_small_collection() {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsCollectionSmall());
        verify(mockDispatcher, never()).dispatchLogs(any(List.class), any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_limit_sized_collection() {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsCollection());
        verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_limit_sized_collection_with_dlq() throws Exception {
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService(dlqPushHandler);
        cloudWatchLogsService.processLogEvents(getSampleRecordsCollection());
        verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class));
        verify(dlqPushHandler, never()).perform(any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_limit_sized_collection_with_large_record_dlq() throws Exception {
        PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn("test");
        when(pluginSetting.getPipelineName()).thenReturn("test");
        when(dlqPushHandler.getPluginSetting()).thenReturn(pluginSetting);
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService(dlqPushHandler);
        cloudWatchLogsService.processLogEvents(List.of(getLargeRecord(2*thresholdConfig.getMaxEventSizeBytes())));
        verify(mockDispatcher, never()).dispatchLogs(any(List.class), any(List.class));
        verify(dlqPushHandler, atLeast(1)).perform(any(List.class));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_limit_sized_collection_with_large_record_no_dlq_are_dropped() throws Exception {
        PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn("test");
        when(pluginSetting.getPipelineName()).thenReturn("test");
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService(null);
        cloudWatchLogsService.processLogEvents(List.of(getLargeRecord(2*thresholdConfig.getMaxEventSizeBytes())));
        verify(mockDispatcher, never()).dispatchLogs(any(List.class), any(List.class));
        verify(eventHandle, times(1)).release(eq(true));
        verify(cloudWatchLogsMetrics, times(1)).increaseLogLargeEventsDroppedCounter(eq(1));
    }

    @Test
    void SHOULD_call_dispatcher_WHEN_process_log_events_called_with_dispatcher_throws_sending_events_to_dlq() throws Exception {
        PluginSetting pluginSetting = mock(PluginSetting.class);
        doAnswer(a -> {
            throw new RuntimeException("failed to dispatch");
        }).when(mockDispatcher).dispatchLogs(any(List.class), any(List.class));
        when(pluginSetting.getName()).thenReturn("test");
        when(pluginSetting.getPipelineName()).thenReturn("test");
        when(dlqPushHandler.getPluginSetting()).thenReturn(pluginSetting);
        setUpRealBuffer();
        cloudWatchLogsService = getSampleService(dlqPushHandler);
        cloudWatchLogsService.processLogEvents(List.of(getLargeRecord(2*thresholdConfig.getMaxEventSizeBytes())));
        verify(mockDispatcher, never()).dispatchLogs(any(List.class), any(List.class));
        verify(dlqPushHandler, atLeast(1)).perform(any(List.class));
    }

    @Test
    void SHOULD_not_call_buffer_WHEN_process_log_events_called_with_limit_sized_records() {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsOfLimitSize());
        verify(buffer, never()).writeEvent(any(EventHandle.class), any(byte[].class));
    }
    
    @Test
    void SHOULD_call_buffer_WHEN_process_log_events_called_with_larger_sized_records() {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        cloudWatchLogsService.processLogEvents(getSampleRecordsOfLargerSize());
        verify(buffer, atLeast(1)).writeEvent(any(EventHandle.class), any(byte[].class));
    }

    //Multithreaded tests:
    void setUpThreadsProcessingLogsWithNormalSample(final int numberOfThreads) throws InterruptedException {
        Thread[] threads = new Thread[numberOfThreads];
        Collection<Record<Event>> sampleEvents = getSampleRecordsCollection();

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i] = new Thread(() -> {
                cloudWatchLogsService.processLogEvents(sampleEvents);
            });
            threads[i].start();
        }

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i].join();
        }
    }

    @Test
    void GIVEN_large_thread_count_WHEN_processing_log_events_THEN_buffer_should_be_called_large_thread_count_times() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        setUpThreadsProcessingLogsWithNormalSample(LARGE_THREAD_COUNT);

        verify(buffer, atLeast(LARGE_THREAD_COUNT)).getBufferedData();
    }

    @Test
    void GIVEN_large_thread_count_WHEN_processing_log_events_THEN_dispatcher_should_be_called_large_thread_count_times() throws InterruptedException {
        setUpSpyBuffer();
        cloudWatchLogsService = getSampleService();
        setUpThreadsProcessingLogsWithNormalSample(LARGE_THREAD_COUNT);

        verify(mockDispatcher, atLeast(LARGE_THREAD_COUNT)).dispatchLogs(any(List.class), any(List.class));
    }

    // --- Dynamic-entity mode -------------------------------------------------

    private CloudWatchLogsService getDynamicService(final EntityResolver entityResolver) {
        return new CloudWatchLogsService(inMemoryBufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                mockDispatcher, null, true, entityResolver);
    }

    private EntityResolver resourceIdResolver() {
        return resourceIdResolver(1000);
    }

    private EntityResolver resourceIdResolver(final int maxCardinality) {
        final Map<String, String> keyAttributes = new java.util.LinkedHashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "${resourceId}");
        return new EntityResolver(keyAttributes, Map.of(), maxCardinality);
    }

    private Collection<Record<Event>> recordsWithResourceId(final String resourceId, final int count) {
        final ArrayList<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final Event event = JacksonLog.builder()
                    .withData(Map.of("resourceId", resourceId, "seq", i))
                    .withEventHandle(mock(EventHandle.class))
                    .build();
            records.add(new Record<>(event));
        }
        return records;
    }

    @Test
    void GIVEN_dynamic_mode_WHEN_events_share_a_resource_id_THEN_dispatch_carries_that_resolved_entity() {
        cloudWatchLogsService = getDynamicService(resourceIdResolver());

        cloudWatchLogsService.processLogEvents(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));

        final ArgumentCaptor<Entity> entityCaptor = ArgumentCaptor.forClass(Entity.class);
        verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class), entityCaptor.capture());
        assertThat(entityCaptor.getValue().keyAttributes().get("Name"), equalTo("res-A"));
    }

    @Test
    void GIVEN_dynamic_mode_WHEN_events_have_distinct_resource_ids_THEN_each_group_dispatches_its_own_entity() {
        cloudWatchLogsService = getDynamicService(resourceIdResolver());

        final Collection<Record<Event>> combined = new ArrayList<>();
        combined.addAll(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));
        combined.addAll(recordsWithResourceId("res-B", thresholdConfig.getBatchSize()));

        cloudWatchLogsService.processLogEvents(combined);

        final ArgumentCaptor<Entity> entityCaptor = ArgumentCaptor.forClass(Entity.class);
        verify(mockDispatcher, atLeast(2)).dispatchLogs(any(List.class), any(List.class), entityCaptor.capture());
        final List<String> dispatchedNames = new ArrayList<>();
        for (final Entity entity : entityCaptor.getAllValues()) {
            dispatchedNames.add(entity.keyAttributes().get("Name"));
        }
        assertThat(dispatchedNames.contains("res-A"), equalTo(true));
        assertThat(dispatchedNames.contains("res-B"), equalTo(true));
        // Each distinct key opens exactly one group.
        verify(cloudWatchLogsMetrics, times(2)).increaseEntityGroupsCreatedCounter(eq(1));
    }

    @Test
    void GIVEN_dynamic_mode_WHEN_event_missing_templated_field_THEN_delivered_via_shared_group_with_empty_name() {
        cloudWatchLogsService = getDynamicService(resourceIdResolver());

        final ArrayList<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
            final Event event = JacksonLog.builder()
                    .withData(Map.of("someOtherField", "value", "seq", i))
                    .withEventHandle(mock(EventHandle.class))
                    .build();
            records.add(new Record<>(event));
        }

        cloudWatchLogsService.processLogEvents(records);

        // The templated ${resourceId} is absent, so every such event resolves to the same key
        // attributes ({Type=Service, Name=}) and shares one group. Events are still delivered; the
        // resolved entity carries an empty Name rather than being dropped.
        final ArgumentCaptor<Entity> entityCaptor = ArgumentCaptor.forClass(Entity.class);
        verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class), entityCaptor.capture());
        assertThat(entityCaptor.getValue().keyAttributes().get("Name"), equalTo(""));
    }

    @Test
    void GIVEN_dynamic_mode_WHEN_many_threads_process_distinct_resource_ids_THEN_all_dispatched_without_races() throws InterruptedException {
        cloudWatchLogsService = getDynamicService(resourceIdResolver());

        final int numberOfThreads = 50;
        final Thread[] threads = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            final String resourceId = "res-" + i;
            threads[i] = new Thread(() ->
                    cloudWatchLogsService.processLogEvents(recordsWithResourceId(resourceId, thresholdConfig.getBatchSize())));
            threads[i].start();
        }
        for (int i = 0; i < numberOfThreads; i++) {
            threads[i].join();
        }

        verify(mockDispatcher, atLeast(numberOfThreads)).dispatchLogs(any(List.class), any(List.class), any(Entity.class));
        verify(cloudWatchLogsMetrics, times(numberOfThreads)).increaseEntityGroupsCreatedCounter(eq(1));
    }

    @Test
    void GIVEN_dynamic_mode_WHEN_distinct_keys_exceed_max_cardinality_THEN_groups_are_bounded_and_overflow_uses_fallback() {
        // Bound of 2: the first two distinct resource ids each open their own group; every further
        // distinct id must reuse the shared fallback group rather than growing the groups map.
        final int maxCardinality = 2;
        cloudWatchLogsService = getDynamicService(resourceIdResolver(maxCardinality));

        final Collection<Record<Event>> combined = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            combined.addAll(recordsWithResourceId("res-" + i, thresholdConfig.getBatchSize()));
        }

        cloudWatchLogsService.processLogEvents(combined);

        // Despite five distinct keys, group creation is bounded: maxCardinality named groups plus
        // one shared fallback group for the overflow. Without the bound this would be five.
        verify(cloudWatchLogsMetrics, times(maxCardinality + 1)).increaseEntityGroupsCreatedCounter(eq(1));
        verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class), any(Entity.class));
    }

     private Record<Event> getLargeRecord(long size) {
        final Event event = JacksonLog.builder().withData(Map.of("key", RandomStringUtils.insecure().nextAlphabetic((int)size))).withEventHandle(eventHandle).build();

        return new Record<>(event);
    }
}
