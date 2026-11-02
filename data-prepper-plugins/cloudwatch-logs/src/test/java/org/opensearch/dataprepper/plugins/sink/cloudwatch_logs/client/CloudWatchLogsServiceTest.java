/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.SinkStopWatch;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudWatchLogsServiceTest {
    private static final long FLUSH_INTERVAL = ThresholdConfig.DEFAULT_FLUSH_INTERVAL;

    private enum BlankEntityField { KEY_ATTRIBUTE, ATTRIBUTE }

    private ThresholdConfig thresholdConfig;
    private CloudWatchLogsLimits cloudWatchLogsLimits;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private InMemoryBufferFactory inMemoryBufferFactory;
    private SinkStopWatch stopWatch;

    @BeforeEach
    void setUp() {
        cloudWatchLogsMetrics = mock(CloudWatchLogsMetrics.class);
        thresholdConfig = new ThresholdConfig();
        cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(), thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(), thresholdConfig.getFlushInterval());
        inMemoryBufferFactory = new InMemoryBufferFactory();
        stopWatch = mock(SinkStopWatch.class);
        when(stopWatch.getElapsedTimeInSeconds()).thenReturn(0L);
    }

    private CloudWatchLogsService staticService(final Buffer buffer) {
        return new CloudWatchLogsService(buffer, cloudWatchLogsMetrics, cloudWatchLogsLimits, null);
    }

    private CloudWatchLogsService staticServiceWithMockedStopWatch() {
        return new CloudWatchLogsService(inMemoryBufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits, null, 0, null,
                () -> stopWatch);
    }

    private static byte[] bytesOf(final Event event) {
        return event.toJsonString().getBytes(StandardCharsets.UTF_8);
    }

    private static Event eventWithMessage(final String message) {
        return JacksonLog.builder()
                .withData(Map.of("message", message))
                .withEventHandle(mock(EventHandle.class))
                .build();
    }

    private static Collection<Record<Event>> records(final int count) {
        final List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new Record<>(JacksonEvent.fromMessage("testMessage")));
        }
        return records;
    }

    private static List<Batch> addAllThenTakeReady(final CloudWatchLogsService service, final Collection<Record<Event>> records) {
        final List<Batch> out = new ArrayList<>();
        for (final Record<Event> record : records) {
            service.add(record.getData(), bytesOf(record.getData()), out);
        }
        service.takeReady(out);
        return out;
    }

    private static List<EventHandle> handlesOf(final List<Batch> batches) {
        final List<EventHandle> handles = new ArrayList<>();
        for (final Batch batch : batches) {
            handles.addAll(batch.getEventHandles());
        }
        return handles;
    }

    @Test
    void add_threeTimesBatchSize_threeBatchesOfBatchSize() {
        final CloudWatchLogsService service = staticServiceWithMockedStopWatch();

        final List<Batch> out = addAllThenTakeReady(service, records(3 * thresholdConfig.getBatchSize()));

        assertThat(out, hasSize(3));
        for (final Batch batch : out) {
            assertThat(batch.getEventHandles(), hasSize(thresholdConfig.getBatchSize()));
            assertThat(batch.getMessages(), hasSize(thresholdConfig.getBatchSize()));
        }
    }

    @Test
    void add_sizeExceeded_splits() {
        final Event first = eventWithMessage("a".repeat(100));
        final int length = bytesOf(first).length;
        cloudWatchLogsLimits = new CloudWatchLogsLimits(100, 1_000_000,
                2L * (length + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE), FLUSH_INTERVAL);
        final CloudWatchLogsService service = staticServiceWithMockedStopWatch();
        final List<Event> events = new ArrayList<>(List.of(first));
        for (int i = 0; i < 4; i++) {
            events.add(eventWithMessage("a".repeat(100)));
        }

        final List<Batch> out = new ArrayList<>();
        for (final Event event : events) {
            service.add(event, bytesOf(event), out);
        }

        assertThat(out, hasSize(2));
        assertThat(out.get(0).getEventHandles(), contains(events.get(0).getEventHandle(), events.get(1).getEventHandle()));
        assertThat(out.get(1).getEventHandles(), contains(events.get(2).getEventHandle(), events.get(3).getEventHandle()));
        for (final Batch batch : out) {
            final long requestSize = batch.getMessages().stream().mapToLong(m -> m.length).sum()
                    + (long) batch.getMessages().size() * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE;
            assertThat(requestSize, lessThanOrEqualTo(2L * (length + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE)));
        }
        final List<Batch> remaining = new ArrayList<>();
        service.takeAll(remaining);
        assertThat(handlesOf(remaining), contains(events.get(4).getEventHandle()));
    }

    @Test
    void add_utf8BytesExceedRequestLimit_takesExistingGroupFirst() {
        // The request size check uses the UTF-8 byte length, not the character count.
        final Event firstEvent = eventWithMessage("ascii");
        final Event nonAsciiEvent = eventWithMessage("\u4E2D".repeat(20));
        final String nonAsciiLogString = nonAsciiEvent.toJsonString();
        final long requestLimitBasedOnCharacterCount = bytesOf(firstEvent).length + nonAsciiLogString.length()
                + 2 * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE;
        cloudWatchLogsLimits = new CloudWatchLogsLimits(100, 1_000_000, requestLimitBasedOnCharacterCount, FLUSH_INTERVAL);
        final Buffer buffer = inMemoryBufferFactory.getBuffer();
        final CloudWatchLogsService service = staticService(buffer);

        final List<Batch> out = addAllThenTakeReady(service, List.of(new Record<>(firstEvent), new Record<>(nonAsciiEvent)));

        assertThat(out, hasSize(1));
        assertThat(out.get(0).getEventHandles(), contains(firstEvent.getEventHandle()));
        assertThat(buffer.getEventCount(), equalTo(1));
        assertThat(buffer.getBufferedData().get(0), equalTo(bytesOf(nonAsciiEvent)));
    }

    @Test
    void add_singleEventLargerThanMaxRequestSize_takenAloneNoEmptyBatch() {
        final Event large = eventWithMessage(RandomStringUtils.insecure().nextAlphabetic(500));
        final Event small = eventWithMessage("small");
        cloudWatchLogsLimits = new CloudWatchLogsLimits(100, 1_000_000, 100, FLUSH_INTERVAL);
        final CloudWatchLogsService service = staticServiceWithMockedStopWatch();

        final List<Batch> out = new ArrayList<>();
        service.add(large, bytesOf(large), out);
        assertThat(out, empty());

        service.add(small, bytesOf(small), out);
        service.takeAll(out);

        assertThat(out, hasSize(2));
        assertThat(out.get(0).getEventHandles(), contains(large.getEventHandle()));
        assertThat(out.get(1).getEventHandles(), contains(small.getEventHandle()));
        for (final Batch batch : out) {
            assertThat(batch.getEventHandles(), not(empty()));
        }
    }

    @Test
    void add_and_takeReady_timeReady_takes() {
        // Time readiness is checked by add for each event, and by takeReady for groups that received no events.
        final CloudWatchLogsService service = staticServiceWithMockedStopWatch();
        final Event first = eventWithMessage("first");
        final Event second = eventWithMessage("second");
        final List<Batch> out = new ArrayList<>();

        when(stopWatch.getElapsedTimeInSeconds()).thenReturn(FLUSH_INTERVAL - 1);
        service.add(first, bytesOf(first), out);
        service.takeReady(out);
        assertThat(out, empty());

        when(stopWatch.getElapsedTimeInSeconds()).thenReturn(FLUSH_INTERVAL);
        service.add(second, bytesOf(second), out);
        assertThat(out, hasSize(1));
        assertThat(out.get(0).getEventHandles(), contains(first.getEventHandle(), second.getEventHandle()));

        final Event third = eventWithMessage("third");
        when(stopWatch.getElapsedTimeInSeconds()).thenReturn(0L);
        service.add(third, bytesOf(third), out);
        service.takeReady(out);
        assertThat(out, hasSize(1));

        when(stopWatch.getElapsedTimeInSeconds()).thenReturn(FLUSH_INTERVAL);
        service.takeReady(out);
        assertThat(out, hasSize(2));
        assertThat(out.get(1).getEventHandles(), contains(third.getEventHandle()));
        verify(stopWatch, times(2)).stopAndReset();
    }

    @Test
    void add_failureAfterWriteEvent_loggedAndEventStaysBuffered() {
        // Once writeEvent returns, the event is owned by the buffer, so add must not throw for it.
        final CloudWatchLogsService service = staticServiceWithMockedStopWatch();
        when(stopWatch.getElapsedTimeInSeconds()).thenThrow(new RuntimeException("stopwatch failed"));
        final Event event = eventWithMessage("buffered");
        final List<Batch> out = new ArrayList<>();

        service.add(event, bytesOf(event), out);

        assertThat(out, empty());
        service.takeAll(out);
        assertThat(handlesOf(out), contains(event.getEventHandle()));
    }

    @Test
    void add_sizeSplitThenWriteEventThrows_takenBatchReachesOut() {
        final Event first = eventWithMessage("a".repeat(100));
        final Event second = eventWithMessage("a".repeat(100));
        cloudWatchLogsLimits = new CloudWatchLogsLimits(100, 1_000_000,
                bytesOf(first).length + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE, FLUSH_INTERVAL);
        final Buffer buffer = spy(InMemoryBuffer.class);
        doCallRealMethod().doThrow(new RuntimeException("write failed")).when(buffer).writeEvent(any(), any());
        final CloudWatchLogsService service = staticService(buffer);
        final List<Batch> out = new ArrayList<>();
        service.add(first, bytesOf(first), out);

        assertThrows(RuntimeException.class, () -> service.add(second, bytesOf(second), out));

        assertThat(out, hasSize(1));
        assertThat(out.get(0).getEventHandles(), contains(first.getEventHandle()));
        assertThat(buffer.getEventCount(), equalTo(0));
    }

    @Test
    void takeReady_take_removesBatchFromGroup() {
        final CloudWatchLogsService service = staticServiceWithMockedStopWatch();
        final List<Batch> out = new ArrayList<>();
        for (final Record<Event> record : records(3)) {
            service.add(record.getData(), bytesOf(record.getData()), out);
        }
        when(stopWatch.getElapsedTimeInSeconds()).thenReturn(FLUSH_INTERVAL);

        service.takeReady(out);
        assertThat(out, hasSize(1));
        assertThat(out.get(0).getEventHandles(), hasSize(3));

        final List<Batch> again = new ArrayList<>();
        service.takeReady(again);
        service.takeAll(again);
        assertThat(again, empty());
    }

    @Test
    void take_thenResetOrAddMore_takenListsUnchanged() {
        final List<EventHandle> handles = new ArrayList<>();
        final List<byte[]> messages = new ArrayList<>();
        final Buffer buffer = mock(Buffer.class);
        when(buffer.getEventCount()).thenAnswer(invocation -> handles.size());
        when(buffer.getEventHandles()).thenReturn(handles);
        when(buffer.getBufferedData()).thenReturn(messages);
        when(buffer.getBufferSize()).thenReturn(42);
        doAnswer(invocation -> {
            handles.add(invocation.getArgument(0));
            messages.add(invocation.getArgument(1));
            return null;
        }).when(buffer).writeEvent(any(), any());
        final CloudWatchLogsService service = new CloudWatchLogsService(() -> buffer, cloudWatchLogsMetrics,
                cloudWatchLogsLimits, null, 0, null, () -> stopWatch);
        final Event first = eventWithMessage("first");
        final Event second = eventWithMessage("second");
        final List<Batch> out = new ArrayList<>();
        service.add(first, bytesOf(first), out);
        service.add(second, bytesOf(second), out);

        service.takeAll(out);

        assertThat(out, hasSize(1));
        final Batch batch = out.get(0);
        handles.clear();
        messages.clear();
        final Event third = eventWithMessage("third");
        service.add(third, bytesOf(third), new ArrayList<>());
        assertThat(batch.getEventHandles(), contains(first.getEventHandle(), second.getEventHandle()));
        assertThat(batch.getMessages(), hasSize(2));
        assertThat(batch.getMessages().get(0), equalTo(bytesOf(first)));
        final InOrder inOrder = inOrder(buffer, stopWatch, cloudWatchLogsMetrics);
        inOrder.verify(buffer).getBufferedData();
        inOrder.verify(buffer).resetBuffer();
        inOrder.verify(stopWatch).stopAndReset();
        inOrder.verify(cloudWatchLogsMetrics).recordRequestSize(42);
        verify(buffer, never()).clearBuffer();
    }

    @Test
    void takeAll_nonEmptyAndEmptyGroups_takesOnlyNonEmptyRegardlessOfReadiness() {
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Name", "${resourceId}");
        final EntityResolver resolver = new EntityResolver(keyAttributes, Map.of(), mock(ExpressionEvaluator.class));
        final CloudWatchLogsService service = new CloudWatchLogsService(inMemoryBufferFactory, cloudWatchLogsMetrics,
                cloudWatchLogsLimits, resolver, EntityConfig.DEFAULT_MAX_CARDINALITY, null, () -> stopWatch);
        final Event dynamicEvent = JacksonLog.builder().withData(Map.of("resourceId", "res-A"))
                .withEventHandle(mock(EventHandle.class)).build();
        final Event sharedEvent = JacksonLog.builder().withData(Map.of("other", "x"))
                .withEventHandle(mock(EventHandle.class)).build();
        final List<Batch> out = new ArrayList<>();
        service.add(dynamicEvent, bytesOf(dynamicEvent), out);
        service.add(sharedEvent, bytesOf(sharedEvent), out);

        service.takeAll(out);

        assertThat(out, hasSize(2));
        assertThat(handlesOf(out), hasItem(dynamicEvent.getEventHandle()));
        assertThat(handlesOf(out), hasItem(sharedEvent.getEventHandle()));
        final List<Batch> again = new ArrayList<>();
        service.takeAll(again);
        assertThat(again, empty());
    }

    @Test
    void takeReady_twoThreadsRacing_eachBatchReturnedOnce() throws InterruptedException {
        final Map<String, String> keyAttributes = new LinkedHashMap<>();
        keyAttributes.put("Name", "${resourceId}");
        final EntityResolver resolver = new EntityResolver(keyAttributes, Map.of(), mock(ExpressionEvaluator.class));
        final CloudWatchLogsService service = new CloudWatchLogsService(inMemoryBufferFactory, cloudWatchLogsMetrics,
                cloudWatchLogsLimits, resolver, EntityConfig.DEFAULT_MAX_CARDINALITY, null, () -> stopWatch);
        final int groupCount = 20;
        final List<EventHandle> allHandles = new ArrayList<>();
        final List<Batch> unused = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            for (int j = 0; j < 3; j++) {
                final Event event = JacksonLog.builder().withData(Map.of("resourceId", "res-" + i, "seq", j))
                        .withEventHandle(mock(EventHandle.class)).build();
                allHandles.add(event.getEventHandle());
                service.add(event, bytesOf(event), unused);
            }
        }
        assertThat(unused, empty());
        when(stopWatch.getElapsedTimeInSeconds()).thenReturn(FLUSH_INTERVAL);

        final ReentrantLock processLock = new ReentrantLock();
        final CountDownLatch start = new CountDownLatch(1);
        final List<Batch> firstOut = new ArrayList<>();
        final List<Batch> secondOut = new ArrayList<>();
        final Thread firstThread = new Thread(() -> takeReadyUnderLock(service, processLock, start, firstOut));
        final Thread secondThread = new Thread(() -> takeReadyUnderLock(service, processLock, start, secondOut));
        firstThread.start();
        secondThread.start();
        start.countDown();
        firstThread.join();
        secondThread.join();

        final List<Batch> combined = new ArrayList<>(firstOut);
        combined.addAll(secondOut);
        assertThat(combined, hasSize(groupCount));
        final Set<EventHandle> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (final EventHandle handle : handlesOf(combined)) {
            assertThat(seen.add(handle), equalTo(true));
        }
        assertThat(seen.size(), equalTo(allHandles.size()));
    }

    private static void takeReadyUnderLock(final CloudWatchLogsService service, final ReentrantLock processLock,
                                           final CountDownLatch start, final List<Batch> out) {
        try {
            start.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        processLock.lock();
        try {
            service.takeReady(out);
        } finally {
            processLock.unlock();
        }
    }

    @Test
    void add_staticMode_batchCarriesStaticEntityAndNoHeaders() {
        final Entity staticEntity = Entity.builder().keyAttributes(Map.of("Identifier", "fixed")).build();
        final CloudWatchLogsService service = new CloudWatchLogsService(inMemoryBufferFactory.getBuffer(), cloudWatchLogsMetrics,
                cloudWatchLogsLimits, staticEntity);

        final List<Batch> out = addAllThenTakeReady(service, records(thresholdConfig.getBatchSize()));

        assertThat(out, hasSize(1));
        assertThat(out.get(0).getEntity(), sameInstance(staticEntity));
        assertThat(out.get(0).getHeaders(), nullValue());
    }

    @Nested
    class DynamicEntityMode {
        private CloudWatchLogsService service;

        private CloudWatchLogsService dynamicService(final EntityResolver entityResolver) {
            return dynamicService(entityResolver, EntityConfig.DEFAULT_MAX_CARDINALITY);
        }

        private CloudWatchLogsService dynamicService(final EntityResolver entityResolver, final int maxCardinality) {
            return new CloudWatchLogsService(inMemoryBufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                    entityResolver, maxCardinality, null);
        }

        private EntityResolver resourceIdResolver() {
            final Map<String, String> keyAttributes = new LinkedHashMap<>();
            keyAttributes.put("Type", "Service");
            keyAttributes.put("Name", "${resourceId}");
            // A plain field reference, so the evaluator is never consulted.
            return new EntityResolver(keyAttributes, Map.of(), mock(ExpressionEvaluator.class));
        }

        private Event eventWithResourceId(final String resourceId, final int seq) {
            return JacksonLog.builder()
                    .withData(Map.of("resourceId", resourceId, "seq", seq))
                    .withEventHandle(mock(EventHandle.class))
                    .build();
        }

        private Collection<Record<Event>> recordsWithResourceId(final String resourceId, final int count) {
            final ArrayList<Record<Event>> records = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                records.add(new Record<>(eventWithResourceId(resourceId, i)));
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

        /** Skips batches with a null entity, which come from the shared group. */
        private List<String> entityNames(final List<Batch> batches) {
            final List<String> names = new ArrayList<>();
            for (final Batch batch : batches) {
                if (batch.getEntity() != null) {
                    names.add(batch.getEntity().keyAttributes().get("Name"));
                }
            }
            return names;
        }

        private List<Entity> entities(final List<Batch> batches) {
            final List<Entity> entities = new ArrayList<>();
            for (final Batch batch : batches) {
                entities.add(batch.getEntity());
            }
            return entities;
        }

        @Test
        void add_distinctResourceIds_eachGroupBatchCarriesItsOwnEntity() {
            service = dynamicService(resourceIdResolver());

            final Collection<Record<Event>> combined = new ArrayList<>();
            combined.addAll(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));
            combined.addAll(recordsWithResourceId("res-B", thresholdConfig.getBatchSize()));

            final List<String> names = entityNames(addAllThenTakeReady(service, combined));

            assertThat(names, hasItem("res-A"));
            assertThat(names, hasItem("res-B"));
            // Each distinct key opens exactly one group.
            verify(cloudWatchLogsMetrics, times(2)).increaseEntityGroupsCreatedCounter(eq(1));
        }

        @Test
        void add_resolveKeyThrows_groupStateUnchanged() {
            final EntityResolver resolver = spy(resourceIdResolver());
            service = dynamicService(resolver);
            final Event buffered = eventWithResourceId("res-A", 0);
            final Event failing = eventWithResourceId("res-B", 1);
            doThrow(new RuntimeException("resolve failed")).when(resolver).resolveKey(failing);
            final List<Batch> out = new ArrayList<>();
            service.add(buffered, bytesOf(buffered), out);

            assertThrows(RuntimeException.class, () -> service.add(failing, bytesOf(failing), out));

            assertThat(out, empty());
            assertThat(service.activeEntityGroupCount(), equalTo(1));
            verify(cloudWatchLogsMetrics, times(1)).increaseEntityGroupsCreatedCounter(eq(1));
            service.takeAll(out);
            assertThat(handlesOf(out), contains(buffered.getEventHandle()));
        }

        private Collection<Record<Event>> recordsWithoutResourceId(final int count) {
            final ArrayList<Record<Event>> records = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                final Event event = JacksonLog.builder()
                        .withData(Map.of("ResourceId", "res-A", "seq", i))
                        .withEventHandle(mock(EventHandle.class))
                        .build();
                records.add(new Record<>(event));
            }
            return records;
        }

        private EntityResolver resourceIdWithCategoryAttributeResolver() {
            final Map<String, String> keyAttributes = new LinkedHashMap<>();
            keyAttributes.put("Type", "Service");
            keyAttributes.put("Name", "${resourceId}");
            return new EntityResolver(keyAttributes, Map.of("Category", "${category}"), mock(ExpressionEvaluator.class));
        }

        @ParameterizedTest
        @EnumSource(BlankEntityField.class)
        void add_eventMissingTemplatedField_takenViaSharedGroupWithNoEntity(final BlankEntityField blankField) {
            final Collection<Record<Event>> records;
            if (blankField == BlankEntityField.KEY_ATTRIBUTE) {
                service = dynamicService(resourceIdResolver());
                records = recordsWithoutResourceId(thresholdConfig.getBatchSize());
            } else {
                service = dynamicService(resourceIdWithCategoryAttributeResolver());
                records = recordsWithResourceId("res-A", thresholdConfig.getBatchSize());
            }

            final List<Batch> out = addAllThenTakeReady(service, records);

            assertThat(out, not(empty()));
            assertThat(entities(out), everyItem(nullValue()));
            verify(cloudWatchLogsMetrics, times(thresholdConfig.getBatchSize()))
                    .increaseEntityDroppedCounter(EntityResolver.BLANK_VALUE);
            verify(cloudWatchLogsMetrics, never()).increaseEntityGroupsCreatedCounter(anyInt());
            verify(cloudWatchLogsMetrics, never()).increaseEntityOverflowEventsCounter(anyInt());
            assertThat(service.activeEntityGroupCount(), equalTo(0));
        }

        @Test
        void add_maxCardinalityOneAndInvalidEntitiesFirst_theyTakeNoSlotAndAreNotOverflow() {
            service = dynamicService(resourceIdResolver(), 1);

            final Collection<Record<Event>> combined = new ArrayList<>();
            combined.addAll(recordsWithoutResourceId(thresholdConfig.getBatchSize()));
            combined.addAll(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));

            final List<Batch> out = addAllThenTakeReady(service, combined);

            verify(cloudWatchLogsMetrics, times(1)).increaseEntityGroupsCreatedCounter(eq(1));
            verify(cloudWatchLogsMetrics, never()).increaseEntityOverflowEventsCounter(anyInt());
            assertThat(entityNames(out), hasItem("res-A"));
        }

        @ParameterizedTest
        @EnumSource(BlankEntityField.class)
        void add_maxCardinalityReachedAndNewKeyHasBlankField_countedAsDroppedNotOverflow(final BlankEntityField blankField) {
            final Collection<Record<Event>> combined = new ArrayList<>();
            if (blankField == BlankEntityField.KEY_ATTRIBUTE) {
                service = dynamicService(resourceIdResolver(), 1);
                combined.addAll(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));
                combined.addAll(recordsWithoutResourceId(thresholdConfig.getBatchSize()));
            } else {
                service = dynamicService(resourceIdWithCategoryAttributeResolver(), 1);
                combined.addAll(recordsWith("res-A", "cat-1", thresholdConfig.getBatchSize()));
                combined.addAll(recordsWithResourceId("res-B", thresholdConfig.getBatchSize()));
            }

            addAllThenTakeReady(service, combined);

            verify(cloudWatchLogsMetrics, times(thresholdConfig.getBatchSize()))
                    .increaseEntityDroppedCounter(EntityResolver.BLANK_VALUE);
            verify(cloudWatchLogsMetrics, never()).increaseEntityOverflowEventsCounter(anyInt());
        }

        @Test
        void add_attributeMissingOnlyOnSomeEventsOfAKey_checkedOnlyWhenOpeningTheGroup() {
            service = dynamicService(resourceIdWithCategoryAttributeResolver());

            final Collection<Record<Event>> combined = new ArrayList<>();
            combined.addAll(recordsWithResourceId("res-A", 1));
            combined.addAll(recordsWith("res-A", "cat-1", thresholdConfig.getBatchSize()));
            combined.addAll(recordsWithResourceId("res-A", 1));

            final List<Batch> out = addAllThenTakeReady(service, combined);

            verify(cloudWatchLogsMetrics, times(1)).increaseEntityDroppedCounter(EntityResolver.BLANK_VALUE);
            verify(cloudWatchLogsMetrics, times(1)).increaseEntityGroupsCreatedCounter(eq(1));
            final List<String> categories = new ArrayList<>();
            for (final Entity entity : entities(out)) {
                if (entity != null) {
                    categories.add(entity.attributes().get("Category"));
                }
            }
            assertThat(categories, hasItem("cat-1"));
        }

        @Test
        void add_distinctKeysExceedMaxCardinality_groupsBoundedAndOverflowUsesFallback() {
            // Bound of 2: the first two distinct resource ids each open their own group; every further
            // distinct id must reuse the shared fallback group rather than growing the groups map.
            final int maxCardinality = 2;
            service = dynamicService(resourceIdResolver(), maxCardinality);

            final int distinctKeys = 5;
            final Collection<Record<Event>> combined = new ArrayList<>();
            for (int i = 0; i < distinctKeys; i++) {
                combined.addAll(recordsWithResourceId("res-" + i, thresholdConfig.getBatchSize()));
            }

            final List<Batch> out = addAllThenTakeReady(service, combined);

            verify(cloudWatchLogsMetrics, times(maxCardinality)).increaseEntityGroupsCreatedCounter(eq(1));
            final int overflowEvents = (distinctKeys - maxCardinality) * thresholdConfig.getBatchSize();
            verify(cloudWatchLogsMetrics, times(overflowEvents)).increaseEntityOverflowEventsCounter(eq(1));
            // Overflow events must not be labelled with some arbitrary resource's id, so the shared batch's entity is null.
            assertThat(entities(out), hasItem(nullValue()));
            assertThat(entities(out), hasItem(notNullValue()));
        }

        @Test
        void takeReady_maxCardinalityOneAfterOverflow_freedSlotStillAdmitsANewEntity() {
            cloudWatchLogsLimits = zeroFlushIntervalLimits();
            // The tightest possible bound, which is where holding the shared group inside the groups map
            // used to be fatal: the first overflow would take the single slot and never release it.
            service = dynamicService(resourceIdResolver(), 1);

            final Collection<Record<Event>> combined = new ArrayList<>();
            combined.addAll(recordsWithResourceId("res-A", thresholdConfig.getBatchSize()));
            combined.addAll(recordsWithResourceId("res-B", thresholdConfig.getBatchSize()));
            addAllThenTakeReady(service, combined);

            // res-A took the slot and res-B overflowed. The sweep then dropped res-A's drained group, and the
            // shared group lives outside the map so it holds no slot.
            verify(cloudWatchLogsMetrics, times(1)).increaseEntityGroupsCreatedCounter(eq(1));
            verify(cloudWatchLogsMetrics, times(thresholdConfig.getBatchSize())).increaseEntityOverflowEventsCounter(eq(1));
            assertThat(service.activeEntityGroupCount(), equalTo(0));

            final List<Batch> out = addAllThenTakeReady(service, recordsWithResourceId("res-C", thresholdConfig.getBatchSize()));

            verify(cloudWatchLogsMetrics, times(2)).increaseEntityGroupsCreatedCounter(eq(1));
            assertThat(entityNames(out), hasItem("res-C"));
        }

        @Test
        void takeReady_idleEmptyDynamicGroup_evictedSharedGroupKept() {
            final InMemoryBufferFactory bufferFactory = spy(new InMemoryBufferFactory());
            service = new CloudWatchLogsService(bufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                    resourceIdResolver(), 1, null, () -> stopWatch);
            final List<Batch> out = new ArrayList<>();
            final Event groupA = eventWithResourceId("res-A", 0);
            final Event overflow = eventWithResourceId("res-B", 1);
            service.add(groupA, bytesOf(groupA), out);
            service.add(overflow, bytesOf(overflow), out);
            service.takeAll(out);
            assertThat(out, hasSize(2));

            when(stopWatch.getElapsedTimeInSeconds()).thenReturn(FLUSH_INTERVAL - 1);
            service.takeReady(out);
            assertThat(service.activeEntityGroupCount(), equalTo(1));

            when(stopWatch.getElapsedTimeInSeconds()).thenReturn(FLUSH_INTERVAL);
            service.takeReady(out);
            assertThat(service.activeEntityGroupCount(), equalTo(0));
            assertThat(out, hasSize(2));

            final Event invalid = JacksonLog.builder().withData(Map.of("other", "x"))
                    .withEventHandle(mock(EventHandle.class)).build();
            service.add(invalid, bytesOf(invalid), new ArrayList<>());
            verify(bufferFactory, times(2)).getBuffer();
        }

        private EntityResolver headerResolver() {
            final Map<String, String> keyAttributes = new LinkedHashMap<>();
            keyAttributes.put("Name", "${resourceId}");
            return new EntityResolver(keyAttributes, Map.of(), Map.of("x-source-type", "${category}"),
                    mock(ExpressionEvaluator.class));
        }

        private Collection<Record<Event>> recordsWith(final String resourceId, final String category, final int count) {
            final ArrayList<Record<Event>> records = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                final Event event = JacksonLog.builder()
                        .withData(Map.of("resourceId", resourceId, "category", category, "seq", i))
                        .withEventHandle(mock(EventHandle.class))
                        .build();
                records.add(new Record<>(event));
            }
            return records;
        }

        @Test
        void add_dynamicHeadersAndResolvedEntity_batchCarriesBothTheHeadersAndTheEntity() {
            service = dynamicService(headerResolver());

            final List<Batch> out = addAllThenTakeReady(service, recordsWith("res-A", "cat-1", thresholdConfig.getBatchSize()));

            assertThat(out, hasSize(1));
            assertThat(out.get(0).getEntity().keyAttributes().get("Name"), equalTo("res-A"));
            assertThat(out.get(0).getHeaders().get("x-source-type"), equalTo("cat-1"));
        }

        @Test
        void add_dynamicHeadersAndStaticEntity_eachGroupUsesTheStaticEntity() {
            final Entity staticEntity = Entity.builder().keyAttributes(Map.of("Identifier", "fixed")).build();
            final EntityResolver resolver = new EntityResolver(Map.of(), Map.of(), Map.of("x-source-type", "${category}"),
                    mock(ExpressionEvaluator.class));
            service = new CloudWatchLogsService(inMemoryBufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                    resolver, EntityConfig.DEFAULT_MAX_CARDINALITY, staticEntity);

            final List<Batch> out = addAllThenTakeReady(service, recordsWith("res-A", "cat-1", thresholdConfig.getBatchSize()));

            assertThat(out, hasSize(1));
            assertThat(out.get(0).getEntity().keyAttributes().get("Identifier"), equalTo("fixed"));
            assertThat(out.get(0).getHeaders().get("x-source-type"), equalTo("cat-1"));
        }
    }
}
