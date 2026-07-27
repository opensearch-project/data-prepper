/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
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
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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

    @Nested
    class DynamicEntityMode {
        private CloudWatchLogsService service;

        private CloudWatchLogsService dynamicService(final EntityResolver entityResolver) {
            return dynamicService(entityResolver, EntityConfig.DEFAULT_MAX_CARDINALITY);
        }

        private CloudWatchLogsService dynamicService(final EntityResolver entityResolver, final int maxCardinality) {
            return new CloudWatchLogsService(inMemoryBufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                    mockDispatcher, null, true, entityResolver, maxCardinality);
        }

        private EntityResolver resourceIdResolver() {
            final Map<String, String> keyAttributes = new LinkedHashMap<>();
            keyAttributes.put("Type", "Service");
            keyAttributes.put("Name", "${resourceId}");
            // A plain field reference, so the evaluator is never consulted.
            return new EntityResolver(keyAttributes, Map.of(), mock(ExpressionEvaluator.class));
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

        /**
         * Limits whose flush interval is zero, so every group is immediately past its time limit. This is
         * what lets the timer-driven paths be exercised without sleeping in a test.
         */
        private CloudWatchLogsLimits zeroFlushIntervalLimits() {
            return new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                    thresholdConfig.getMaxRequestSizeBytes(), 0);
        }

        /**
         * The {@code Name} key attribute of every entity dispatched so far. The shared group dispatches a
         * null entity, which contributes no name.
         */
        private List<String> dispatchedEntityNames() {
            final ArgumentCaptor<Entity> entityCaptor = ArgumentCaptor.forClass(Entity.class);
            verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class), entityCaptor.capture());
            final List<String> names = new ArrayList<>();
            for (final Entity entity : entityCaptor.getAllValues()) {
                if (entity != null) {
                    names.add(entity.keyAttributes().get("Name"));
                }
            }
            return names;
        }

        @Test
        void GIVEN_dynamic_mode_WHEN_events_share_a_resource_id_THEN_dispatch_carries_that_resolved_entity() {
            service = dynamicService(resourceIdResolver());

            service.processLogEvents(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));

            assertThat(dispatchedEntityNames().contains("res-A"), equalTo(true));
        }

        @Test
        void GIVEN_dynamic_mode_WHEN_events_have_distinct_resource_ids_THEN_each_group_dispatches_its_own_entity() {
            service = dynamicService(resourceIdResolver());

            final Collection<Record<Event>> combined = new ArrayList<>();
            combined.addAll(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));
            combined.addAll(recordsWithResourceId("res-B", thresholdConfig.getBatchSize()));

            service.processLogEvents(combined);

            final List<String> dispatchedNames = dispatchedEntityNames();
            assertThat(dispatchedNames.contains("res-A"), equalTo(true));
            assertThat(dispatchedNames.contains("res-B"), equalTo(true));
            // Each distinct key opens exactly one group.
            verify(cloudWatchLogsMetrics, times(2)).increaseEntityGroupsCreatedCounter(eq(1));
        }

        @Test
        void GIVEN_dynamic_mode_WHEN_key_attributes_resolve_equal_THEN_one_group_serves_both_events() {
            service = dynamicService(resourceIdResolver());

            // Two separate events resolving to the same key attributes. ResolvedKey is the groups-map key,
            // so its equals()/hashCode() are what make these share a group instead of opening two.
            final Collection<Record<Event>> combined = new ArrayList<>();
            combined.addAll(recordsWithResourceId("res-A", 1));
            combined.addAll(recordsWithResourceId("res-A", 1));

            service.processLogEvents(combined);

            verify(cloudWatchLogsMetrics, times(1)).increaseEntityGroupsCreatedCounter(eq(1));
            assertThat(service.activeEntityGroupCount(), equalTo(1));
        }

        @Test
        void GIVEN_dynamic_mode_WHEN_event_missing_templated_field_THEN_delivered_via_shared_group_with_empty_name() {
            service = dynamicService(resourceIdResolver());

            final ArrayList<Record<Event>> records = new ArrayList<>();
            for (int i = 0; i < thresholdConfig.getBatchSize(); i++) {
                final Event event = JacksonLog.builder()
                        .withData(Map.of("someOtherField", "value", "seq", i))
                        .withEventHandle(mock(EventHandle.class))
                        .build();
                records.add(new Record<>(event));
            }

            service.processLogEvents(records);

            // The templated ${resourceId} is absent, so every such event resolves to the same key
            // attributes ({Type=Service, Name=}) and shares one group. Events are still delivered; the
            // resolved entity carries an empty Name rather than being dropped.
            assertThat(dispatchedEntityNames().contains(""), equalTo(true));
        }

        @Test
        void GIVEN_dynamic_mode_WHEN_many_threads_process_distinct_resource_ids_THEN_all_dispatched_without_races() throws InterruptedException {
            service = dynamicService(resourceIdResolver());

            final int numberOfThreads = 50;
            final Thread[] threads = new Thread[numberOfThreads];
            for (int i = 0; i < numberOfThreads; i++) {
                final String resourceId = "res-" + i;
                threads[i] = new Thread(() ->
                        service.processLogEvents(recordsWithResourceId(resourceId, thresholdConfig.getBatchSize())));
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
            service = dynamicService(resourceIdResolver(), maxCardinality);

            final int distinctKeys = 5;
            final Collection<Record<Event>> combined = new ArrayList<>();
            for (int i = 0; i < distinctKeys; i++) {
                combined.addAll(recordsWithResourceId("res-" + i, thresholdConfig.getBatchSize()));
            }

            service.processLogEvents(combined);

            // Despite five distinct keys, group creation is bounded to maxCardinality. The shared fallback
            // is deliberately not counted here, so this stays a count of real entities.
            verify(cloudWatchLogsMetrics, times(maxCardinality)).increaseEntityGroupsCreatedCounter(eq(1));

            // Every event of every overflowing key is counted, so the loss of attribution is visible.
            final int overflowEvents = (distinctKeys - maxCardinality) * thresholdConfig.getBatchSize();
            verify(cloudWatchLogsMetrics, times(overflowEvents)).increaseEntityOverflowEventsCounter(eq(1));

            // The whole point of the fallback: overflow events must not be labelled with some arbitrary
            // resource's id, so the entity dispatched for that group is null.
            verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class), isNull());
            // The two in-bounds keys still dispatch their own real entities.
            verify(mockDispatcher, atLeast(1)).dispatchLogs(any(List.class), any(List.class), any(Entity.class));
        }

        @Test
        void GIVEN_max_cardinality_of_one_WHEN_overflow_has_happened_THEN_a_freed_slot_still_admits_a_new_entity() {
            cloudWatchLogsLimits = zeroFlushIntervalLimits();
            // The tightest possible bound, which is where holding the shared group inside the groups map
            // used to be fatal: the first overflow would take the single slot and never release it, so
            // every subsequent event of every key overflowed into it for the lifetime of the process.
            service = dynamicService(resourceIdResolver(), 1);

            final Collection<Record<Event>> combined = new ArrayList<>();
            combined.addAll(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));
            combined.addAll(recordsWithResourceId("res-B", thresholdConfig.getBatchSize()));
            service.processLogEvents(combined);

            // res-A took the slot and res-B overflowed. The end-of-batch sweep then dropped res-A's
            // drained group, and the shared group lives outside the map so it holds no slot.
            verify(cloudWatchLogsMetrics, times(1)).increaseEntityGroupsCreatedCounter(eq(1));
            verify(cloudWatchLogsMetrics, times(thresholdConfig.getBatchSize()))
                    .increaseEntityOverflowEventsCounter(eq(1));
            assertThat(service.activeEntityGroupCount(), equalTo(0));

            service.processLogEvents(recordsWithResourceId("res-C", thresholdConfig.getBatchSize()));

            // So res-C gets a real group and its own entity, rather than being absorbed forever.
            verify(cloudWatchLogsMetrics, times(2)).increaseEntityGroupsCreatedCounter(eq(1));
            assertThat(dispatchedEntityNames().contains("res-C"), equalTo(true));
        }

        @Test
        void GIVEN_dynamic_mode_WHEN_a_group_is_left_empty_and_idle_THEN_its_slot_is_released_for_a_new_entity() {
            cloudWatchLogsLimits = zeroFlushIntervalLimits();
            // Bound of 1: without eviction res-A would hold the only slot for the lifetime of the process,
            // and res-B would have to collapse into the null-entity fallback.
            service = dynamicService(resourceIdResolver(), 1);

            service.processLogEvents(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));

            // res-A's buffer was drained and its group is past the flush interval, so the sweep dropped it.
            assertThat(service.activeEntityGroupCount(), equalTo(0));

            service.processLogEvents(recordsWithResourceId("res-B", thresholdConfig.getBatchSize()));

            // Two real groups over the run even though only one could exist at a time, and no overflow.
            verify(cloudWatchLogsMetrics, times(2)).increaseEntityGroupsCreatedCounter(eq(1));
            verify(cloudWatchLogsMetrics, never()).increaseEntityOverflowEventsCounter(anyInt());

            final List<String> dispatchedNames = dispatchedEntityNames();
            assertThat(dispatchedNames.contains("res-A"), equalTo(true));
            assertThat(dispatchedNames.contains("res-B"), equalTo(true));
        }

        @Test
        void GIVEN_dynamic_mode_WHEN_a_group_stays_below_the_count_limit_THEN_the_timer_flushes_it_on_a_non_empty_batch() {
            cloudWatchLogsLimits = zeroFlushIntervalLimits();
            service = dynamicService(resourceIdResolver());

            // Far below batchSize and nowhere near the request size limit, so only the time limit can
            // trigger a flush. The batch is non-empty, so the empty-poll path is never reached.
            final int eventCount = 3;
            service.processLogEvents(recordsWithResourceId("res-A", eventCount));

            // One dispatch per event, from the time-limit check in the hot path. Before that check existed
            // these events would have sat in the buffer for as long as the pipeline kept delivering records.
            verify(mockDispatcher, times(eventCount)).dispatchLogs(any(List.class), any(List.class), any(Entity.class));
        }
    }

     private Record<Event> getLargeRecord(long size) {
        final Event event = JacksonLog.builder().withData(Map.of("key", RandomStringUtils.insecure().nextAlphabetic((int)size))).withEventHandle(eventHandle).build();

        return new Record<>(event);
    }
}
