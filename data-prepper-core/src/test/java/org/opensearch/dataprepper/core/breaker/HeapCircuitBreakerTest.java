/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.breaker;

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

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HeapCircuitBreakerTest {
    private static final Duration VERY_LARGE_RESET_PERIOD = Duration.ofDays(1);
    private static final Duration SMALL_RESET_PERIOD = Duration.ofMillis(50);
    private static final Duration SMALL_CHECK_INTERVAL = SMALL_RESET_PERIOD;
    private static final long SLEEP_MILLIS = SMALL_CHECK_INTERVAL.plusMillis(50).toMillis();
    @Mock
    private HeapCircuitBreakerConfig config;

    @Mock
    private MemoryMXBean memoryMXBean;

    @Mock
    private Runnable garbageCollector;

    private Duration initialGarbageCollectionInterval;
    private Duration maxGarbageCollectionInterval;
    private Random random;
    private long byteUsage;
    private MemoryUsage memoryUsage;

    private HeapCircuitBreaker objectUnderTest;

    @BeforeEach
    void setUp() {
        random = new Random();
        initialGarbageCollectionInterval = VERY_LARGE_RESET_PERIOD;
        maxGarbageCollectionInterval = VERY_LARGE_RESET_PERIOD;
    }

    @AfterEach
    void tearDown() throws Exception {
        if(objectUnderTest != null) {
            objectUnderTest.close();
            objectUnderTest = null;
        }
    }

    private HeapCircuitBreaker createObjectUnderTest() {
        return new HeapCircuitBreaker(config,
                                      memoryMXBean,
                                      garbageCollector,
                                      initialGarbageCollectionInterval,
                                      maxGarbageCollectionInterval);
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
        final ByteCount usageByteCount = mock(ByteCount.class);
        when(usageByteCount.getBytes()).thenReturn(1024L);
        when(config.getUsage()).thenReturn(usageByteCount);
        final ByteCount closeUsageByteCount = mock(ByteCount.class);
        when(closeUsageByteCount.getBytes()).thenReturn(1025L);
        when(config.getCloseUsage()).thenReturn(closeUsageByteCount);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    void constructor_throws_if_closeUsage_is_non_positive(final long bytes) {
        final ByteCount usageByteCount = mock(ByteCount.class);
        when(usageByteCount.getBytes()).thenReturn(1024L);
        when(config.getUsage()).thenReturn(usageByteCount);
        final ByteCount closeUsageByteCount = mock(ByteCount.class);
        when(closeUsageByteCount.getBytes()).thenReturn(bytes);
        when(config.getCloseUsage()).thenReturn(closeUsageByteCount);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Nested
    class ValidConfig {
        @BeforeEach
        void setUp() {
            byteUsage = random.nextInt(1024) + 1024 * 1024;
            final ByteCount usageByteCount = mock(ByteCount.class);
            when(usageByteCount.getBytes()).thenReturn(byteUsage);
            when(config.getUsage()).thenReturn(usageByteCount);
            when(config.getCheckInterval()).thenReturn(SMALL_CHECK_INTERVAL);

            memoryUsage = mock(MemoryUsage.class);
            when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsage);
        }

        @Test
        void object_checks_memory_even_when_not_calling_isOpen() throws InterruptedException {
            objectUnderTest = createObjectUnderTest();

            Thread.sleep(SLEEP_MILLIS);

            verify(memoryMXBean, atLeastOnce()).getHeapMemoryUsage();
        }

        @ParameterizedTest
        @ValueSource(longs = {1, 2, 1024})
        void isOpen_returns_false_if_used_bytes_less_than_configured_bytes(final long bytesDifference) throws InterruptedException {
            when(memoryUsage.getUsed()).thenReturn(byteUsage - bytesDifference);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_returns_false_if_used_bytes_equal_to_configured_bytes() throws InterruptedException {
            when(memoryUsage.getUsed()).thenReturn(byteUsage);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(longs = {1, 2, 1024, 1024 * 1024})
        void isOpen_returns_true_if_used_bytes_greater_than_configured_bytes(final long bytesGreater) throws InterruptedException {
            when(memoryUsage.getUsed()).thenReturn(byteUsage + bytesGreater);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void will_not_check_within_reset_period() throws InterruptedException {
            when(config.getReset()).thenReturn(VERY_LARGE_RESET_PERIOD);

            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();

            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));

            reset(memoryUsage);
            lenient().when(memoryUsage.getUsed()).thenReturn(byteUsage - 1);
            for(int i = 0; i < 3; i++) {
                Thread.sleep(SLEEP_MILLIS);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void will_check_after_reset_period() throws InterruptedException {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);

            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();

            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));

            reset(memoryUsage);
            when(memoryUsage.getUsed()).thenReturn(byteUsage - 1);
            for(int i = 0; i < 3; i++) {
                Thread.sleep(SLEEP_MILLIS);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_transition_from_false_to_true() throws InterruptedException {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            when(memoryUsage.getUsed()).thenReturn(byteUsage - 1);

            objectUnderTest = createObjectUnderTest();

            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(false));

            reset(memoryUsage);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @Test
        void isOpen_transition_from_true_to_false() throws InterruptedException {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);

            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);
            objectUnderTest = createObjectUnderTest();

            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));

            reset(memoryUsage);
            when(memoryUsage.getUsed()).thenReturn(byteUsage - 1);
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_returns_false_if_MemoryMXBean_throws_on_first_call() throws InterruptedException {
            reset(memoryMXBean);
            when(memoryMXBean.getHeapMemoryUsage()).thenThrow(RuntimeException.class);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_stays_true_while_used_bytes_are_above_closeUsage() throws InterruptedException {
            final long closeUsage = byteUsage - 1024;
            givenCloseUsage(closeUsage);
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));

            reset(memoryUsage);
            when(memoryUsage.getUsed()).thenReturn(closeUsage + 1);
            for(int i = 0; i < 3; i++) {
                Thread.sleep(SLEEP_MILLIS);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(true));
        }

        @ParameterizedTest
        @ValueSource(longs = {0, 1, 1024})
        void isOpen_transition_from_true_to_false_when_used_bytes_are_at_or_below_closeUsage(final long bytesBelowCloseUsage)
                throws InterruptedException {
            final long closeUsage = byteUsage - 1024;
            givenCloseUsage(closeUsage);
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));

            reset(memoryUsage);
            when(memoryUsage.getUsed()).thenReturn(closeUsage - bytesBelowCloseUsage);
            for(int i = 0; i < 3; i++) {
                Thread.sleep(SLEEP_MILLIS);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_returns_false_if_used_bytes_are_between_closeUsage_and_usage_without_tripping() throws InterruptedException {
            final long closeUsage = byteUsage - 1024;
            givenCloseUsage(closeUsage);
            when(memoryUsage.getUsed()).thenReturn(closeUsage + 1);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void isOpen_transition_from_true_to_false_when_closeUsage_equals_usage() throws InterruptedException {
            givenCloseUsage(byteUsage);
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(true));

            reset(memoryUsage);
            when(memoryUsage.getUsed()).thenReturn(byteUsage);
            for(int i = 0; i < 3; i++) {
                Thread.sleep(SLEEP_MILLIS);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(false));
        }

        @Test
        void garbage_collection_is_requested_once_when_tripping() throws InterruptedException {
            when(config.getReset()).thenReturn(VERY_LARGE_RESET_PERIOD);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            verify(garbageCollector, times(1)).run();
        }

        @Test
        void garbage_collection_is_not_requested_while_closed() throws InterruptedException {
            when(memoryUsage.getUsed()).thenReturn(byteUsage - 1);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            verify(garbageCollector, never()).run();
        }

        @Test
        void isOpen_returns_false_if_garbage_collection_before_tripping_frees_memory() throws InterruptedException {
            final AtomicLong usedBytes = new AtomicLong(byteUsage + 1);
            when(memoryUsage.getUsed()).thenAnswer(invocation -> usedBytes.get());
            doAnswer(invocation -> {
                usedBytes.set(byteUsage - 1);
                return null;
            }).when(garbageCollector).run();

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            verify(garbageCollector, times(1)).run();
        }

        @Test
        void isOpen_transition_from_false_to_true_without_garbage_collection_within_garbage_collection_interval()
                throws InterruptedException {
            when(config.getReset()).thenReturn(VERY_LARGE_RESET_PERIOD);
            final AtomicLong usedBytes = new AtomicLong(byteUsage + 1);
            when(memoryUsage.getUsed()).thenAnswer(invocation -> usedBytes.get());
            doAnswer(invocation -> {
                usedBytes.set(byteUsage - 1);
                return null;
            }).when(garbageCollector).run();

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(false));

            usedBytes.set(byteUsage + 1);
            Thread.sleep(SLEEP_MILLIS);

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            verify(garbageCollector, times(1)).run();
        }

        @Test
        void garbage_collection_is_requested_again_before_tripping_after_garbage_collection_interval()
                throws InterruptedException {
            initialGarbageCollectionInterval = SMALL_RESET_PERIOD;
            final AtomicLong usedBytes = new AtomicLong(byteUsage + 1);
            when(memoryUsage.getUsed()).thenAnswer(invocation -> usedBytes.get());
            doAnswer(invocation -> {
                usedBytes.set(byteUsage - 1);
                return null;
            }).when(garbageCollector).run();

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SLEEP_MILLIS);
            assertThat(objectUnderTest.isOpen(), equalTo(false));

            Thread.sleep(SLEEP_MILLIS);
            usedBytes.set(byteUsage + 1);
            Thread.sleep(SLEEP_MILLIS);

            assertThat(objectUnderTest.isOpen(), equalTo(false));
            verify(garbageCollector, times(2)).run();
        }

        @Test
        void garbage_collection_is_not_retried_before_garbage_collection_interval() throws InterruptedException {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();
            for(int i = 0; i < 3; i++) {
                Thread.sleep(SLEEP_MILLIS);
            }

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            verify(garbageCollector, times(1)).run();
        }

        @Test
        void isOpen_transition_from_true_to_false_when_retried_garbage_collection_frees_memory() throws InterruptedException {
            final long closeUsage = byteUsage - 1024;
            givenCloseUsage(closeUsage);
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            initialGarbageCollectionInterval = SMALL_RESET_PERIOD;
            final AtomicLong usedBytes = new AtomicLong(byteUsage + 1);
            when(memoryUsage.getUsed()).thenAnswer(invocation -> usedBytes.get());
            final AtomicBoolean memoryReleased = new AtomicBoolean(false);
            doAnswer(invocation -> {
                if(memoryReleased.get()) {
                    usedBytes.set(closeUsage - 1);
                }
                return null;
            }).when(garbageCollector).run();

            objectUnderTest = createObjectUnderTest();
            await().atMost(Duration.ofSeconds(2)).until(objectUnderTest::isOpen);
            verify(garbageCollector, atLeastOnce()).run();

            memoryReleased.set(true);

            await().atMost(Duration.ofSeconds(2)).until(() -> !objectUnderTest.isOpen());
            assertThat(usedBytes.get(), equalTo(closeUsage - 1));
            verify(garbageCollector, atLeast(2)).run();
        }

        @Test
        void garbage_collection_interval_backs_off_while_garbage_collection_does_not_free_enough_memory() throws InterruptedException {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            initialGarbageCollectionInterval = SMALL_RESET_PERIOD;
            maxGarbageCollectionInterval = SMALL_RESET_PERIOD.multipliedBy(8);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            objectUnderTest = createObjectUnderTest();
            Thread.sleep(SMALL_RESET_PERIOD.multipliedBy(24).toMillis());

            assertThat(objectUnderTest.isOpen(), equalTo(true));
            final int garbageCollections = mockingDetails(garbageCollector).getInvocations().size();
            assertThat(garbageCollections, greaterThanOrEqualTo(3));
            assertThat(garbageCollections, lessThanOrEqualTo(10));
        }

        @Test
        void garbage_collection_interval_stays_at_maximum_once_reached() {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);
            initialGarbageCollectionInterval = Duration.ofMillis(150);
            maxGarbageCollectionInterval = Duration.ofMillis(300);
            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);
            final List<Long> garbageCollectionTimes = new CopyOnWriteArrayList<>();
            doAnswer(invocation -> {
                garbageCollectionTimes.add(System.nanoTime());
                return null;
            }).when(garbageCollector).run();

            objectUnderTest = createObjectUnderTest();
            await().atMost(Duration.ofSeconds(5)).until(() -> garbageCollectionTimes.size() >= 6);

            // Without the cap the intervals would continue with 600 ms and 1200 ms.
            final long maxMillis = maxGarbageCollectionInterval.toMillis();
            for (int i = 2; i < 6; i++) {
                final long intervalMillis = TimeUnit.NANOSECONDS.toMillis(
                        garbageCollectionTimes.get(i) - garbageCollectionTimes.get(i - 1));
                assertThat(intervalMillis, greaterThanOrEqualTo(maxMillis - SMALL_CHECK_INTERVAL.toMillis()));
                assertThat(intervalMillis, lessThan(maxMillis + 3 * SMALL_CHECK_INTERVAL.toMillis()));
            }
        }

        private void givenCloseUsage(final long closeUsageBytes) {
            final ByteCount closeUsageByteCount = mock(ByteCount.class);
            when(closeUsageByteCount.getBytes()).thenReturn(closeUsageBytes);
            when(config.getCloseUsage()).thenReturn(closeUsageByteCount);
        }
    }
}
