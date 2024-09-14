/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MultiBufferDecoratorTest {
    private static final Random RANDOM = new Random();
    private static final int TIMEOUT_MILLIS = RANDOM.nextInt(1000000) + 1;

    @Mock
    private Buffer primaryBuffer;

    @Mock
    private Buffer secondaryBuffer;

    @Mock
    private Record record;

    @Mock
    private CheckpointState checkpointState;

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(primaryBuffer, secondaryBuffer);
    }

    @Test
    void write_should_call_primary_buffer_write() throws TimeoutException {
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(new Random().nextInt(10) + 1);

        multiBufferDecorator.write(record, TIMEOUT_MILLIS);
        verify(primaryBuffer).write(record, TIMEOUT_MILLIS);
    }

    @Test
    void writeAll_should_call_primary_buffer_writeAll() throws Exception {
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(new Random().nextInt(10) + 1);

        multiBufferDecorator.writeAll(List.of(record), TIMEOUT_MILLIS);
        verify(primaryBuffer).writeAll(List.of(record), TIMEOUT_MILLIS);
    }

    @Test
    void read_should_call_primary_buffer_read() {
        final Map.Entry<Collection<Record>, CheckpointState> entry =
                new AbstractMap.SimpleEntry<Collection<Record>, CheckpointState>(List.of(record), checkpointState);
        when(primaryBuffer.read(TIMEOUT_MILLIS)).thenReturn(entry);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(new Random().nextInt(10) + 1);

        final Map.Entry<Collection<Record>, CheckpointState> result = multiBufferDecorator.read(TIMEOUT_MILLIS);
        assertThat(result, equalTo(entry));

        verify(primaryBuffer).read(TIMEOUT_MILLIS);
    }

    @Test
    void checkpoint_should_call_primary_buffer_checkpoint() {
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(new Random().nextInt(10) + 1);

        multiBufferDecorator.checkpoint(checkpointState);
        verify(primaryBuffer).checkpoint(checkpointState);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 46})
    void isEmpty_AllBuffersAreEmpty(final int secondaryBufferCount) {
        when(primaryBuffer.isEmpty()).thenReturn(true);
        when(secondaryBuffer.isEmpty()).thenReturn(true);

        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(secondaryBufferCount);

        final boolean result = multiBufferDecorator.isEmpty();
        assertThat(result, equalTo(true));

        verify(primaryBuffer).isEmpty();
        verify(secondaryBuffer, times(secondaryBufferCount)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isEmpty_NoSecondaryBuffers(final boolean isPrimaryBufferEmpty) {
        when(primaryBuffer.isEmpty()).thenReturn(isPrimaryBufferEmpty);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(0);

        final boolean result = multiBufferDecorator.isEmpty();
        assertThat(result, equalTo(isPrimaryBufferEmpty));

        verify(primaryBuffer).isEmpty();
    }

    @Test
    void isEmpty_PrimaryBufferNotEmpty() {
        when(primaryBuffer.isEmpty()).thenReturn(false);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(1);

        final boolean result = multiBufferDecorator.isEmpty();
        assertThat(result, equalTo(false));

        verify(primaryBuffer).isEmpty();
    }

    @Test
    void isEmpty_SecondaryBufferNotEmpty() {
        when(primaryBuffer.isEmpty()).thenReturn(true);
        when(secondaryBuffer.isEmpty()).thenReturn(false);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(1);

        final boolean result = multiBufferDecorator.isEmpty();
        assertThat(result, equalTo(false));

        verify(primaryBuffer).isEmpty();
        verify(secondaryBuffer).isEmpty();
    }

    @Test
    void isEmpty_MultipleSecondaryBuffers_OneNotEmpty() {
        when(primaryBuffer.isEmpty()).thenReturn(true);
        when(secondaryBuffer.isEmpty()).thenReturn(true).thenReturn(false);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(2);

        final boolean result = multiBufferDecorator.isEmpty();
        assertThat(result, equalTo(false));

        verify(primaryBuffer).isEmpty();
        verify(secondaryBuffer, times(2)).isEmpty();
    }

    @Test
    void getDrainTimeout_NoSecondaryBuffers_ReturnsPrimaryBufferValue() {
        final Duration primaryBufferDrainTimeout = Duration.ofMillis(RANDOM.nextLong());
        when(primaryBuffer.getDrainTimeout()).thenReturn(primaryBufferDrainTimeout);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(0);

        final Duration result = multiBufferDecorator.getDrainTimeout();
        assertThat(result, equalTo(primaryBufferDrainTimeout));

        verify(primaryBuffer).getDrainTimeout();
    }

    @Test
    void getDrainTimeout_OneSecondaryBuffer_ReturnsSumOfDurations() {
        final Duration primaryBufferDrainTimeout = Duration.ofMillis(RANDOM.nextLong());
        when(primaryBuffer.getDrainTimeout()).thenReturn(primaryBufferDrainTimeout);
        final Duration secondaryBufferDrainTimeout = Duration.ofMillis(RANDOM.nextLong());
        when(secondaryBuffer.getDrainTimeout()).thenReturn(secondaryBufferDrainTimeout);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(1);

        final Duration result = multiBufferDecorator.getDrainTimeout();
        assertThat(result, equalTo(primaryBufferDrainTimeout.plus(secondaryBufferDrainTimeout)));

        verify(primaryBuffer).getDrainTimeout();
        verify(secondaryBuffer).getDrainTimeout();
    }

    @Test
    void getDrainTimeout_MultipleSecondaryBuffers_ReturnsSumOfDurations() {
        final Duration primaryBufferDrainTimeout = Duration.ofMillis(RANDOM.nextLong());
        when(primaryBuffer.getDrainTimeout()).thenReturn(primaryBufferDrainTimeout);
        final Duration secondaryBufferDrainTimeout1 = Duration.ofMillis(RANDOM.nextLong());
        final Duration secondaryBufferDrainTimeout2 = Duration.ofMillis(RANDOM.nextLong());
        when(secondaryBuffer.getDrainTimeout())
                .thenReturn(secondaryBufferDrainTimeout1)
                .thenReturn(secondaryBufferDrainTimeout2);
        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(2);

        final Duration result = multiBufferDecorator.getDrainTimeout();
        assertThat(result, equalTo(primaryBufferDrainTimeout.plus(secondaryBufferDrainTimeout1).plus(secondaryBufferDrainTimeout2)));

        verify(primaryBuffer).getDrainTimeout();
        verify(secondaryBuffer, times(2)).getDrainTimeout();
    }

    @Test
    void shutdown_NoSecondaryBuffers_CallsPrimaryBufferShutdown() {
        createObjectUnderTest(0).shutdown();
        verify(primaryBuffer).shutdown();
    }

    @Test
    void shutdown_OneSecondaryBuffers_CallsPrimaryAndSecondaryBufferShutdown() {
        createObjectUnderTest(1).shutdown();
        verify(primaryBuffer).shutdown();
        verify(secondaryBuffer).shutdown();
    }

    @Test
    void shutdown_MultipleSecondaryBuffers_CallsAllBuffersShutdown() {
        createObjectUnderTest(2).shutdown();
        verify(primaryBuffer).shutdown();
        verify(secondaryBuffer, times(2)).shutdown();
    }

    @Test
    void test_getMaxRequestSize() {
        when(primaryBuffer.getMaxRequestSize()).thenReturn(Optional.empty());
        when(secondaryBuffer.getMaxRequestSize()).thenReturn(Optional.empty());

        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(2);
        assertThat(multiBufferDecorator.getMaxRequestSize(), equalTo(Optional.empty()));
    }

    @Test
    void test_getOptimalRequestSize() {
        when(primaryBuffer.getOptimalRequestSize()).thenReturn(Optional.empty());
        when(secondaryBuffer.getOptimalRequestSize()).thenReturn(Optional.empty());

        final MultiBufferDecorator multiBufferDecorator = createObjectUnderTest(2);
        assertThat(multiBufferDecorator.getOptimalRequestSize(), equalTo(Optional.empty()));
    }

    private MultiBufferDecorator createObjectUnderTest(final int secondaryBufferCount) {
        final List<Buffer> secondaryBuffers = IntStream.range(0, secondaryBufferCount)
                .mapToObj(i -> secondaryBuffer)
                .collect(Collectors.toList());

        return new MultiBufferDecorator(primaryBuffer, secondaryBuffers);
    }
}
