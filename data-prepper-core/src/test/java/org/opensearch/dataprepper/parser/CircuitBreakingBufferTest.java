/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CircuitBreakingBufferTest {
    @Mock
    private Buffer<Record<?>> buffer;

    @Mock
    private CircuitBreaker circuitBreaker;

    private int timeoutMillis;

    @BeforeEach
    void setUp() {
        timeoutMillis = 1000;
    }

    private CircuitBreakingBuffer<Record<?>> createObjectUnderTest() {
        return new CircuitBreakingBuffer<>(buffer, circuitBreaker);
    }

    @Test
    void constructor_should_throw_with_null_buffer() {
        buffer = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_with_null_circuitBreaker() {
        circuitBreaker = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getDrainTimeout_returns_buffer_drain_timeout() {
        final Duration duration = Duration.ofMillis(new Random().nextLong());
        when(buffer.getDrainTimeout()).thenReturn(duration);

        final Duration result = createObjectUnderTest().getDrainTimeout();
        assertThat(result, equalTo(duration));
    }

    @Test
    void shutdown_calls_buffer_shutdown() {
        createObjectUnderTest().shutdown();
        verify(buffer).shutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isByteBuffer_returns_value_of_inner_buffer(boolean innerIsByteBuffer) {
        when(buffer.isByteBuffer()).thenReturn(innerIsByteBuffer);

        assertThat(createObjectUnderTest().isByteBuffer(), equalTo(innerIsByteBuffer));
    }

    @Nested
    class NoCircuitBreakerChecks {
        @AfterEach
        void verifyNoCircuitBreakerChecks() {
            verifyNoInteractions(circuitBreaker);
        }

        @Test
        void read_should_return_inner_read() {
            final Map.Entry<Collection<Record<?>>, CheckpointState> readResponse = mock(Map.Entry.class);
            when(buffer.read(timeoutMillis)).thenReturn(readResponse);
            createObjectUnderTest().read(timeoutMillis);
        }

        @Test
        void checkpoint_should_call_inner_checkpoint() {
            final CheckpointState checkpointState = mock(CheckpointState.class);
            createObjectUnderTest().checkpoint(checkpointState);

            verify(buffer).checkpoint(checkpointState);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void isEmpty_should_return_inner_isEmpty(final boolean empty) {
            when(buffer.isEmpty()).thenReturn(empty);

            assertThat(createObjectUnderTest().isEmpty(), equalTo(empty));
        }
    }

    @Nested
    class WithRecords {
        @Mock
        private Record<?> record;

        @Test
        void write_should_check_CircuitBreaker_and_call_inner_write_if_not_open() throws TimeoutException {
            when(circuitBreaker.isOpen()).thenReturn(false);

            createObjectUnderTest().write(record, timeoutMillis);

            verify(buffer).write(record, timeoutMillis);
            verify(circuitBreaker).isOpen();
        }

        @Test
        void write_should_check_CircuitBreaker_and_throw_if_open() {
            when(circuitBreaker.isOpen()).thenReturn(true);

            assertThrows(TimeoutException.class, () -> createObjectUnderTest().write(record, timeoutMillis));

            verifyNoInteractions(buffer);
            verify(circuitBreaker).isOpen();
        }

        @Test
        void writeAll_should_check_CircuitBreaker_and_call_inner_write_if_not_open() throws Exception {
            when(circuitBreaker.isOpen()).thenReturn(false);

            final List<Record<?>> records = Collections.singletonList(record);
            createObjectUnderTest().writeAll(records, timeoutMillis);

            verify(buffer).writeAll(records, timeoutMillis);
            verify(circuitBreaker).isOpen();
        }

        @Test
        void writeAll_should_check_CircuitBreaker_and_throw_if_open() {
            when(circuitBreaker.isOpen()).thenReturn(true);

            final List<Record<?>> records = Collections.singletonList(record);
            assertThrows(TimeoutException.class, () -> createObjectUnderTest().writeAll(records, timeoutMillis));

            verifyNoInteractions(buffer);
            verify(circuitBreaker).isOpen();
        }
    }

    @Nested
    class WithBytes {
        private byte[] bytes;

        private String key;

        @BeforeEach
        void setUp() {
            bytes = UUID.randomUUID().toString().getBytes();
            key = UUID.randomUUID().toString();
        }

        @Test
        void writeBytes_should_check_CircuitBreaker_and_call_inner_write_if_not_open() throws Exception {
            when(circuitBreaker.isOpen()).thenReturn(false);

            createObjectUnderTest().writeBytes(bytes, key, timeoutMillis);

            verify(buffer).writeBytes(bytes, key, timeoutMillis);
            verify(circuitBreaker).isOpen();
        }

        @Test
        void writeBytes_should_check_CircuitBreaker_and_throw_if_open() {
            when(circuitBreaker.isOpen()).thenReturn(true);

            CircuitBreakingBuffer<Record<?>> objectUnderTest = createObjectUnderTest();
            assertThrows(TimeoutException.class, () -> objectUnderTest.writeBytes(bytes, key, timeoutMillis));

            verifyNoInteractions(buffer);
            verify(circuitBreaker).isOpen();
        }
    }
}