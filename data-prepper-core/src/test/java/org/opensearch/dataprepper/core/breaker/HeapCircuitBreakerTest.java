/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.dataprepper.core.breaker.HeapCircuitBreaker;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.core.parser.model.HeapCircuitBreakerConfig;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
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

    private Random random;
    private long byteUsage;
    private MemoryUsage memoryUsage;

    private HeapCircuitBreaker objectUnderTest;

    @BeforeEach
    void setUp() {
        random = new Random();
    }

    @AfterEach
    void tearDown() throws Exception {
        if(objectUnderTest != null) {
            objectUnderTest.close();
            objectUnderTest = null;
        }
    }

    private HeapCircuitBreaker createObjectUnderTest() {
        return new HeapCircuitBreaker(config, memoryMXBean);
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
    }
}