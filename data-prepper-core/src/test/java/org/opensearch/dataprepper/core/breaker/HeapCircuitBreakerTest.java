/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.breaker;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.model.HeapCircuitBreakerConfig;
import org.opensearch.dataprepper.model.types.ByteCount;

import javax.management.ObjectName;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The breaker runs its check on a {@link ManualScheduler} and reads the time from a {@link MutableClock},
 * so each test triggers the checks and advances the time explicitly instead of sleeping.
 */
@ExtendWith(MockitoExtension.class)
class HeapCircuitBreakerTest {
    private static final Duration CHECK_INTERVAL = Duration.ofMillis(500);
    private static final Duration RESET_PERIOD = Duration.ofSeconds(1);
    private static final Duration INITIAL_GARBAGE_COLLECTION_INTERVAL = Duration.ofSeconds(5);
    private static final Duration MAX_GARBAGE_COLLECTION_INTERVAL = Duration.ofSeconds(60);

    @Mock
    private HeapCircuitBreakerConfig config;

    private FakeMemoryMXBean memoryMXBean;
    private MutableClock clock;
    private ManualScheduler scheduler;
    private MeterRegistry meterRegistry;
    private List<Instant> garbageCollectionTimes;
    private List<Boolean> openDuringGarbageCollection;
    private Long bytesAfterGarbageCollection;
    private RuntimeException garbageCollectionFailure;

    private HeapCircuitBreaker objectUnderTest;

    @BeforeEach
    void setUp() {
        memoryMXBean = new FakeMemoryMXBean();
        clock = new MutableClock(Instant.parse("2026-11-06T10:00:00Z"));
        scheduler = new ManualScheduler();
        meterRegistry = new SimpleMeterRegistry();
        garbageCollectionTimes = new ArrayList<>();
        openDuringGarbageCollection = new ArrayList<>();
        bytesAfterGarbageCollection = null;
        garbageCollectionFailure = null;
    }

    @AfterEach
    void tearDown() throws Exception {
        if(objectUnderTest != null) {
            objectUnderTest.close();
            objectUnderTest = null;
        }
        scheduler.shutdownNow();
    }

    private HeapCircuitBreaker createObjectUnderTest() {
        return new HeapCircuitBreaker(config,
                                      memoryMXBean,
                                      this::collectGarbage,
                                      meterRegistry,
                                      clock,
                                      scheduler,
                                      INITIAL_GARBAGE_COLLECTION_INTERVAL,
                                      MAX_GARBAGE_COLLECTION_INTERVAL);
    }

    private void collectGarbage() {
        garbageCollectionTimes.add(clock.instant());
        openDuringGarbageCollection.add(objectUnderTest.isOpen());
        if(garbageCollectionFailure != null) {
            throw garbageCollectionFailure;
        }
        if(bytesAfterGarbageCollection != null) {
            memoryMXBean.setUsedBytes(bytesAfterGarbageCollection);
        }
    }

    @Test
    void constructor_throws_if_config_is_null() {
        config = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_usage_is_null() {
        lenient().when(config.getUsage()).thenReturn(null);
        lenient().when(config.getReset()).thenReturn(Duration.ofSeconds(1));
        lenient().when(config.getCheckInterval()).thenReturn(Duration.ofSeconds(1));
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_reset_is_null() {
        lenient().when(config.getCheckInterval()).thenReturn(Duration.ofSeconds(1));
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_checkInterval_is_null() {
        lenient().when(config.getReset()).thenReturn(Duration.ofSeconds(1));
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    void constructor_throws_if_usage_is_non_positive(final long bytes) {
        final ByteCount byteCount = mock(ByteCount.class);
        when(byteCount.getBytes()).thenReturn(bytes);
        when(config.getUsage()).thenReturn(byteCount);
        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_closeUsage_is_greater_than_usage() {
        when(config.getUsage()).thenReturn(ByteCount.ofBytes(1024L));
        when(config.getCloseUsage()).thenReturn(ByteCount.ofBytes(1025L));

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    void constructor_throws_if_closeUsage_is_non_positive(final long bytes) {
        when(config.getUsage()).thenReturn(ByteCount.ofBytes(1024L));
        final ByteCount closeUsageByteCount = mock(ByteCount.class);
        when(closeUsageByteCount.getBytes()).thenReturn(bytes);
        when(config.getCloseUsage()).thenReturn(closeUsageByteCount);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Nested
    class ValidConfig {
        private long byteUsage;
        private Long closeUsage;

        @BeforeEach
        void setUp() {
            byteUsage = new Random().nextInt(1024) + 1024 * 1024;
            closeUsage = null;
        }

        private void createValidObjectUnderTest() {
            when(config.getUsage()).thenReturn(ByteCount.ofBytes(byteUsage));
            when(config.getCloseUsage()).thenReturn(closeUsage == null ? null : ByteCount.ofBytes(closeUsage));
            when(config.getReset()).thenReturn(RESET_PERIOD);
            when(config.getCheckInterval()).thenReturn(CHECK_INTERVAL);
            objectUnderTest = createObjectUnderTest();
        }

        /**
         * Opens the breaker with a garbage collection that does not free anything.
         */
        private void givenOpenBreaker() {
            memoryMXBean.setUsedBytes(byteUsage + 1);
            createValidObjectUnderTest();
            scheduler.tick();
            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void schedules_the_memory_check_at_the_check_interval() {
            createValidObjectUnderTest();

            assertThat(scheduler.initialDelayMillis, equalTo(0L));
            assertThat(scheduler.periodMillis, equalTo(CHECK_INTERVAL.toMillis()));
        }

        @Test
        void close_shuts_down_the_scheduler() throws Exception {
            createValidObjectUnderTest();

            objectUnderTest.close();

            assertThat(scheduler.isShutdown(), equalTo(true));
        }

        @Test
        void isOpen_returns_false_before_the_first_check() {
            memoryMXBean.setUsedBytes(byteUsage + 1);
            createValidObjectUnderTest();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(longs = {1, 2, 1024})
        void isOpen_returns_false_if_used_bytes_less_than_configured_bytes(final long bytesDifference) {
            memoryMXBean.setUsedBytes(byteUsage - bytesDifference);
            createValidObjectUnderTest();

            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_returns_false_if_used_bytes_equal_to_configured_bytes() {
            memoryMXBean.setUsedBytes(byteUsage);
            createValidObjectUnderTest();

            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(longs = {1, 2, 1024, 1024 * 1024})
        void isOpen_returns_true_if_used_bytes_greater_than_configured_bytes(final long bytesGreater) {
            memoryMXBean.setUsedBytes(byteUsage + bytesGreater);
            createValidObjectUnderTest();

            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void isOpen_returns_false_if_MemoryMXBean_throws_on_first_call() {
            memoryMXBean.failure = new RuntimeException("heap usage not available");
            createValidObjectUnderTest();

            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void check_does_not_throw_and_next_check_runs_if_MemoryMXBean_throws() {
            memoryMXBean.failure = new RuntimeException("heap usage not available");
            memoryMXBean.setUsedBytes(byteUsage + 1);
            createValidObjectUnderTest();
            scheduler.tick();

            memoryMXBean.failure = null;
            clock.advance(CHECK_INTERVAL);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void breaker_stays_open_and_retries_garbage_collection_if_garbage_collection_throws() {
            memoryMXBean.setUsedBytes(byteUsage + 1);
            garbageCollectionFailure = new RuntimeException("garbage collection failed");
            createValidObjectUnderTest();
            scheduler.tick();
            assertThat(objectUnderTest.isOpen(), equalTo(true));

            garbageCollectionFailure = null;
            bytesAfterGarbageCollection = byteUsage - 1;
            clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            assertThat(garbageCollectionTimes.size(), equalTo(2));
        }

        @Test
        void will_not_check_within_reset_period() {
            givenOpenBreaker();
            memoryMXBean.setUsedBytes(byteUsage - 1);

            clock.advance(RESET_PERIOD.minusMillis(1));
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void will_check_after_reset_period() {
            givenOpenBreaker();
            memoryMXBean.setUsedBytes(byteUsage - 1);

            clock.advance(RESET_PERIOD);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_transition_from_false_to_true() {
            memoryMXBean.setUsedBytes(byteUsage - 1);
            createValidObjectUnderTest();
            scheduler.tick();
            assertThat(objectUnderTest.isOpen(), equalTo(false));

            memoryMXBean.setUsedBytes(byteUsage + 1);
            clock.advance(CHECK_INTERVAL);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void isOpen_stays_true_while_used_bytes_are_above_closeUsage() {
            closeUsage = byteUsage - 1024;
            givenOpenBreaker();
            memoryMXBean.setUsedBytes(closeUsage + 1);

            for(int i = 0; i < 3; i++) {
                clock.advance(RESET_PERIOD);
                scheduler.tick();
            }

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @ParameterizedTest
        @ValueSource(longs = {0, 1, 1024})
        void isOpen_transition_from_true_to_false_when_used_bytes_are_at_or_below_closeUsage(final long bytesBelowCloseUsage) {
            closeUsage = byteUsage - 1024;
            givenOpenBreaker();
            memoryMXBean.setUsedBytes(closeUsage - bytesBelowCloseUsage);

            clock.advance(RESET_PERIOD);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_returns_false_if_used_bytes_are_between_closeUsage_and_usage_without_tripping() {
            closeUsage = byteUsage - 1024;
            memoryMXBean.setUsedBytes(closeUsage + 1);
            createValidObjectUnderTest();

            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_transition_from_true_to_false_when_closeUsage_equals_usage() {
            closeUsage = byteUsage;
            givenOpenBreaker();
            memoryMXBean.setUsedBytes(byteUsage);

            clock.advance(RESET_PERIOD);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void breaker_opens_before_the_garbage_collection_runs() {
            givenOpenBreaker();

            assertThat(openDuringGarbageCollection, contains(true));
        }

        @Test
        void garbage_collection_is_requested_once_when_tripping() {
            givenOpenBreaker();

            assertThat(garbageCollectionTimes.size(), equalTo(1));
        }

        @Test
        void garbage_collection_is_not_requested_while_closed() {
            memoryMXBean.setUsedBytes(byteUsage);
            createValidObjectUnderTest();

            for(int i = 0; i < 3; i++) {
                scheduler.tick();
                clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            assertThat(garbageCollectionTimes, empty());
        }

        @Test
        void breaker_closes_in_the_same_check_if_garbage_collection_frees_memory_to_close_threshold() {
            closeUsage = byteUsage - 1024;
            memoryMXBean.setUsedBytes(byteUsage + 1);
            bytesAfterGarbageCollection = closeUsage;
            createValidObjectUnderTest();

            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            assertThat(openDuringGarbageCollection, contains(true));
            assertThat(tripCount(), equalTo(1.0));
        }

        @Test
        void breaker_stays_open_if_garbage_collection_frees_memory_only_below_usage() {
            closeUsage = byteUsage - 1024;
            memoryMXBean.setUsedBytes(byteUsage + 1);
            bytesAfterGarbageCollection = closeUsage + 1;
            createValidObjectUnderTest();

            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void breaker_opens_without_garbage_collection_within_garbage_collection_interval() {
            memoryMXBean.setUsedBytes(byteUsage + 1);
            bytesAfterGarbageCollection = byteUsage - 1;
            createValidObjectUnderTest();
            scheduler.tick();
            assertThat(objectUnderTest.isOpen(), equalTo(false));

            memoryMXBean.setUsedBytes(byteUsage + 1);
            clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL.minusMillis(1));
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            assertThat(garbageCollectionTimes.size(), equalTo(1));
            assertThat(tripCount(), equalTo(2.0));
        }

        @Test
        void garbage_collection_is_requested_again_when_tripping_after_garbage_collection_interval() {
            memoryMXBean.setUsedBytes(byteUsage + 1);
            bytesAfterGarbageCollection = byteUsage - 1;
            createValidObjectUnderTest();
            scheduler.tick();
            assertThat(objectUnderTest.isOpen(), equalTo(false));

            memoryMXBean.setUsedBytes(byteUsage + 1);
            clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            assertThat(garbageCollectionTimes.size(), equalTo(2));
            assertThat(openDuringGarbageCollection, contains(true, true));
        }

        @Test
        void garbage_collection_is_not_retried_before_garbage_collection_interval() {
            givenOpenBreaker();

            final Instant nextGarbageCollection = clock.instant().plus(INITIAL_GARBAGE_COLLECTION_INTERVAL);
            clock.advance(CHECK_INTERVAL);
            while(clock.instant().isBefore(nextGarbageCollection)) {
                scheduler.tick();
                clock.advance(CHECK_INTERVAL);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            assertThat(garbageCollectionTimes.size(), equalTo(1));
        }

        @Test
        void isOpen_transition_from_true_to_false_when_retried_garbage_collection_frees_memory() {
            closeUsage = byteUsage - 1024;
            givenOpenBreaker();
            bytesAfterGarbageCollection = closeUsage - 1;

            clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            assertThat(garbageCollectionTimes.size(), equalTo(2));
        }

        @Test
        void garbage_collection_interval_backs_off_up_to_the_maximum_while_garbage_collection_does_not_free_enough_memory() {
            givenOpenBreaker();

            while(clock.elapsed().compareTo(Duration.ofSeconds(260)) < 0) {
                clock.advance(CHECK_INTERVAL);
                scheduler.tick();
            }

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            final List<Long> secondsSinceTrip = new ArrayList<>();
            for(final Instant garbageCollectionTime : garbageCollectionTimes) {
                secondsSinceTrip.add(Duration.between(clock.start, garbageCollectionTime).toSeconds());
            }
            assertThat(secondsSinceTrip, contains(0L, 5L, 15L, 35L, 75L, 135L, 195L, 255L));
        }

        @Test
        void garbage_collection_interval_starts_at_the_initial_interval_when_tripping_again() {
            closeUsage = byteUsage - 1024;
            givenOpenBreaker();
            clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL);
            scheduler.tick();
            clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL.multipliedBy(2));
            scheduler.tick();
            assertThat(garbageCollectionTimes.size(), equalTo(3));

            memoryMXBean.setUsedBytes(closeUsage);
            clock.advance(CHECK_INTERVAL);
            scheduler.tick();
            assertThat(objectUnderTest.isOpen(), equalTo(false));

            memoryMXBean.setUsedBytes(byteUsage + 1);
            clock.advance(INITIAL_GARBAGE_COLLECTION_INTERVAL);
            scheduler.tick();

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            assertThat(garbageCollectionTimes.size(), equalTo(4));
        }

        @Test
        void metrics_report_open_state_trips_and_garbage_collections() {
            givenOpenBreaker();
            assertThat(meterRegistry.get(HeapCircuitBreaker.OPEN_METRIC).gauge().value(),
                    equalTo((double) HeapCircuitBreaker.OPEN_METRIC_VALUE));
            assertThat(meterRegistry.get(HeapCircuitBreaker.MEMORY_USAGE_METRIC).gauge().value(),
                    equalTo((double) (byteUsage + 1)));

            memoryMXBean.setUsedBytes(byteUsage);
            clock.advance(RESET_PERIOD);
            scheduler.tick();

            assertThat(meterRegistry.get(HeapCircuitBreaker.OPEN_METRIC).gauge().value(),
                    equalTo((double) HeapCircuitBreaker.CLOSED_METRIC_VALUE));
            assertThat(tripCount(), equalTo(1.0));
            assertThat(meterRegistry.get(HeapCircuitBreaker.GARBAGE_COLLECTION_METRIC).timer().count(), equalTo(1L));
        }

        private double tripCount() {
            return meterRegistry.get(HeapCircuitBreaker.TRIPS_METRIC).counter().count();
        }
    }

    /**
     * Captures the task that the breaker schedules and runs it only when the test calls {@link #tick()}.
     */
    private static class ManualScheduler extends ScheduledThreadPoolExecutor {
        private Runnable task;
        private long initialDelayMillis;
        private long periodMillis;

        ManualScheduler() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay,
                                                      final long period, final TimeUnit unit) {
            task = command;
            initialDelayMillis = unit.toMillis(initialDelay);
            periodMillis = unit.toMillis(period);
            return null;
        }

        void tick() {
            task.run();
        }
    }

    private static class MutableClock extends Clock {
        private final Instant start;
        private Instant now;

        MutableClock(final Instant start) {
            this.start = start;
            this.now = start;
        }

        void advance(final Duration duration) {
            now = now.plus(duration);
        }

        Duration elapsed() {
            return Duration.between(start, now);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private static class FakeMemoryMXBean implements MemoryMXBean {
        private final AtomicLong usedBytes = new AtomicLong();
        private RuntimeException failure;

        void setUsedBytes(final long bytes) {
            usedBytes.set(bytes);
        }

        @Override
        public MemoryUsage getHeapMemoryUsage() {
            if(failure != null) {
                throw failure;
            }
            return new MemoryUsage(0, usedBytes.get(), Long.MAX_VALUE, -1);
        }

        @Override
        public MemoryUsage getNonHeapMemoryUsage() {
            return new MemoryUsage(0, 0, 0, -1);
        }

        @Override
        @SuppressWarnings("deprecation")
        public int getObjectPendingFinalizationCount() {
            return 0;
        }

        @Override
        public boolean isVerbose() {
            return false;
        }

        @Override
        public void setVerbose(final boolean value) {
        }

        @Override
        public void gc() {
        }

        @Override
        public ObjectName getObjectName() {
            return null;
        }
    }
}
