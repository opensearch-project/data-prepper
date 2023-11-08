/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DelegatingBufferTest {
    @Mock
    private Buffer<Record<String>> innerBuffer;

    @Mock
    private Record<String> record;

    private Collection<Record<String>> records;

    private int timeoutInMillis;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
        timeoutInMillis = random.nextInt(10_000) + 100;

        records = List.of(record, mock(Record.class));
    }

    private static class TestDelegatingBuffer extends DelegatingBuffer<Record<String>> {
        TestDelegatingBuffer(final Buffer<Record<String>> delegateBuffer) {
            super(delegateBuffer);
        }
    }

    private DelegatingBuffer<Record<String>> createObjectUnderTest() {
        return new TestDelegatingBuffer(innerBuffer);
    }

    @Test
    void constructor_throws_if_delegate_is_null() {
        innerBuffer = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void write_calls_inner_write() throws TimeoutException {
        createObjectUnderTest().write(record, timeoutInMillis);

        verify(innerBuffer).write(record, timeoutInMillis);
    }

    @ParameterizedTest
    @ValueSource(classes = {RuntimeException.class, TimeoutException.class})
    void write_throws_exceptions_from_inner_write(final Class<Throwable> exceptionType) throws TimeoutException {
        final Throwable exception = mock(exceptionType);
        doThrow(exception).when(innerBuffer).write(any(), anyInt());

        final DelegatingBuffer<Record<String>> objectUnderTest = createObjectUnderTest();
        final Exception actualException = assertThrows(Exception.class, () -> objectUnderTest.write(record, timeoutInMillis));

        assertThat(actualException, sameInstance(exception));
    }

    @Test
    void writeAll_calls_inner_writeAll() throws Exception {
        createObjectUnderTest().writeAll(records, timeoutInMillis);

        verify(innerBuffer).writeAll(records, timeoutInMillis);
    }

    @ParameterizedTest
    @ValueSource(classes = {Exception.class, RuntimeException.class, TimeoutException.class})
    void writeAll_throws_exceptions_from_inner_writeAll(final Class<Throwable> exceptionType) throws Exception {
        final Throwable exception = mock(exceptionType);
        doThrow(exception).when(innerBuffer).writeAll(any(), anyInt());

        final DelegatingBuffer<Record<String>> objectUnderTest = createObjectUnderTest();
        final Exception actualException = assertThrows(Exception.class, () -> objectUnderTest.writeAll(records, timeoutInMillis));

        assertThat(actualException, sameInstance(exception));
    }

    @Test
    void writeBytes_calls_inner_writeBytes() throws Exception {
        final byte[] bytesToWrite = new byte[64];
        random.nextBytes(bytesToWrite);
        final String key = UUID.randomUUID().toString();
        createObjectUnderTest().writeBytes(bytesToWrite, key, timeoutInMillis);

        verify(innerBuffer).writeBytes(bytesToWrite, key, timeoutInMillis);
    }

    @ParameterizedTest
    @ValueSource(classes = {Exception.class, RuntimeException.class, TimeoutException.class})
    void writeBytes_throws_exceptions_from_inner_writeBytes(final Class<Throwable> exceptionType) throws Exception {
        final Throwable exception = mock(exceptionType);
        doThrow(exception).when(innerBuffer).writeBytes(any(), any(), anyInt());

        final byte[] bytesToWrite = new byte[64];
        random.nextBytes(bytesToWrite);
        final String key = UUID.randomUUID().toString();

        final DelegatingBuffer<Record<String>> objectUnderTest = createObjectUnderTest();
        final Exception actualException = assertThrows(Exception.class, () -> objectUnderTest.writeBytes(bytesToWrite, key, timeoutInMillis));

        assertThat(actualException, sameInstance(exception));
    }

    @Test
    void read_returns_inner_read() {
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = mock(Map.Entry.class);
        when(innerBuffer.read(timeoutInMillis)).thenReturn(readResult);

        assertThat(createObjectUnderTest().read(timeoutInMillis),
                equalTo(readResult));
    }

    @Test
    void read_throws_exceptions_from_inner_read() {
        final RuntimeException exception = mock(RuntimeException.class);

        when(innerBuffer.read(timeoutInMillis)).thenThrow(exception);

        final DelegatingBuffer<Record<String>> objectUnderTest = createObjectUnderTest();
        final Exception actualException = assertThrows(Exception.class, () -> objectUnderTest.read(timeoutInMillis));

        assertThat(actualException, sameInstance(exception));
    }

    @Test
    void checkpoint_calls_inner_checkpoint() {
        final CheckpointState checkpointState = mock(CheckpointState.class);

        createObjectUnderTest().checkpoint(checkpointState);

        verify(innerBuffer).checkpoint(checkpointState);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isEmpty_returns_inner_isEmpty(final boolean isEmpty) {
        when(innerBuffer.isEmpty()).thenReturn(isEmpty);

        assertThat(createObjectUnderTest().isEmpty(),
                equalTo(isEmpty));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isByteBuffer_returns_inner_isByteBuffer(final boolean isByteBuffer) {
        when(innerBuffer.isByteBuffer()).thenReturn(isByteBuffer);

        assertThat(createObjectUnderTest().isByteBuffer(),
                equalTo(isByteBuffer));
    }

    @Test
    void getDrainTimeout_returns_inner_getDrainTimeout() {
        final Duration drainTimeout = Duration.ofSeconds(random.nextInt(10_000) + 100);
        when(innerBuffer.getDrainTimeout()).thenReturn(drainTimeout);

        assertThat(createObjectUnderTest().getDrainTimeout(),
                equalTo(drainTimeout));
    }

    @Test
    void shutdown_calls_inner_shutdown() {
        createObjectUnderTest().shutdown();

        verify(innerBuffer).shutdown();
    }
}