/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.breaker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.parser.model.HeapCircuitBreakerConfig;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HeapCircuitBreakerTest {
    private static final Duration VERY_LARGE_RESET_PERIOD = Duration.ofDays(1);
    private static final Duration SMALL_RESET_PERIOD = Duration.ofMillis(200);
    @Mock
    private HeapCircuitBreakerConfig config;

    @Mock
    private MemoryMXBean memoryMXBean;

    private Random random;
    private long byteUsage;
    private MemoryUsage memoryUsage;

    @BeforeEach
    void setUp() {
        random = new Random();
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
        when(config.getUsage()).thenReturn(null);
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

            memoryUsage = mock(MemoryUsage.class);
            when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsage);
        }

        @ParameterizedTest
        @ValueSource(longs = {1, 2, 1024})
        void isOpen_returns_false_if_used_bytes_less_than_configured_bytes(final long bytesDifference) {
            when(memoryUsage.getUsed()).thenReturn(byteUsage - bytesDifference);

            assertThat(createObjectUnderTest().isOpen(), equalTo(false));
        }

        @Test
        void isOpen_returns_false_if_used_bytes_equal_to_configured_bytes() {
            when(memoryUsage.getUsed()).thenReturn(byteUsage);

            assertThat(createObjectUnderTest().isOpen(), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(longs = {1, 2, 1024, 1024 * 1024})
        void isOpen_returns_true_if_used_bytes_greater_than_configured_bytes(final long bytesGreater) {
            when(memoryUsage.getUsed()).thenReturn(byteUsage + bytesGreater);

            assertThat(createObjectUnderTest().isOpen(), equalTo(true));
        }

        @Test
        void isOpen_called_multiple_times_within_reset_period_when_below_threshold_will_check_memory_each_time() {
            when(config.getReset()).thenReturn(VERY_LARGE_RESET_PERIOD);

            when(memoryUsage.getUsed()).thenReturn(byteUsage - 1);

            final HeapCircuitBreaker objectUnderTest = createObjectUnderTest();

            final int timesToCheckIsOpen = 200;
            for (int i = 0; i < timesToCheckIsOpen; i++) {
                objectUnderTest.isOpen();
            }

            verify(memoryMXBean, times(timesToCheckIsOpen)).getHeapMemoryUsage();
        }

        @Test
        void isOpen_called_multiple_times_within_reset_period_when_used_bytes_less_than_configured_will_not_call_again() {
            when(config.getReset()).thenReturn(VERY_LARGE_RESET_PERIOD);

            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            final HeapCircuitBreaker objectUnderTest = createObjectUnderTest();
            objectUnderTest.isOpen();

            for (int i = 0; i < 200; i++) {
                objectUnderTest.isOpen();
            }

            verify(memoryMXBean, times(1)).getHeapMemoryUsage();
        }

        @Test
        void isOpen_called_twice_after_reset_period_will_call_again() throws InterruptedException {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);

            when(memoryUsage.getUsed()).thenReturn(byteUsage + 1);

            final HeapCircuitBreaker objectUnderTest = createObjectUnderTest();
            objectUnderTest.isOpen();
            Thread.sleep(SMALL_RESET_PERIOD.toMillis());
            objectUnderTest.isOpen();

            verify(memoryMXBean, times(2)).getHeapMemoryUsage();
        }

        @Test
        void isOpen_transition_from_false_to_true() {
            final HeapCircuitBreaker objectUnderTest = createObjectUnderTest();

            when(memoryUsage.getUsed())
                    .thenReturn(byteUsage - 1)
                    .thenReturn(byteUsage + 1);
            assertThat(objectUnderTest.isOpen(), equalTo(false));
            assertThat(objectUnderTest.isOpen(), equalTo(true));
            verify(memoryMXBean, times(2)).getHeapMemoryUsage();
        }

        @Test
        void isOpen_transition_from_true_to_false() throws InterruptedException {
            when(config.getReset()).thenReturn(SMALL_RESET_PERIOD);

            final HeapCircuitBreaker objectUnderTest = createObjectUnderTest();

            when(memoryUsage.getUsed())
                    .thenReturn(byteUsage + 1)
                    .thenReturn(byteUsage - 1);
            assertThat(objectUnderTest.isOpen(), equalTo(true));
            Thread.sleep(SMALL_RESET_PERIOD.toMillis());
            assertThat(objectUnderTest.isOpen(), equalTo(false));
            verify(memoryMXBean, times(2)).getHeapMemoryUsage();
        }

        @Test
        void isOpen_returns_false_if_MemoryMXBean_throws_on_first_call() {
            reset(memoryMXBean);
            when(memoryMXBean.getHeapMemoryUsage()).thenThrow(RuntimeException.class);

            assertThat(createObjectUnderTest().isOpen(), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(longs = {1, -1})
        void isOpen_returns_previous_state_if_MemoryMXBean_throws_on_first_call(final long difference) {
            when(config.getReset()).thenReturn(Duration.ZERO);
            reset(memoryMXBean);
            when(memoryUsage.getUsed())
                    .thenReturn(byteUsage + difference)
                    .thenReturn(byteUsage - difference);
            when(memoryMXBean.getHeapMemoryUsage())
                    .thenReturn(memoryUsage)
                    .thenThrow(RuntimeException.class);

            final HeapCircuitBreaker objectUnderTest = createObjectUnderTest();
            final boolean previousOpen = objectUnderTest.isOpen();
            assertThat(objectUnderTest.isOpen(), equalTo(previousOpen));

            verify(memoryMXBean, times(2)).getHeapMemoryUsage();
        }
    }
}