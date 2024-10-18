/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.DataPrepperShutdownOptions;
import org.opensearch.dataprepper.model.buffer.Buffer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineShutdownTest {
    @Mock
    private Buffer<?> buffer;

    @Mock
    private Clock clock;

    @Mock
    private DataPrepperShutdownOptions dataPrepperShutdownOptions;

    private Duration bufferDrainTimeout;
    private Random random;
    private String pipelineName;

    @BeforeEach
    void setUp() {
        random = new Random();
        pipelineName = UUID.randomUUID().toString();
        bufferDrainTimeout = Duration.ofSeconds(random.nextInt(100) + 1_000);

        when(buffer.getDrainTimeout()).thenReturn(bufferDrainTimeout);
    }

    private PipelineShutdown createObjectUnderTest() {
        return new PipelineShutdown(pipelineName, buffer, clock);
    }

    @Test
    void constructor_throws_if_drainTimeout_is_null() {
        reset(buffer);
        when(buffer.getDrainTimeout()).thenReturn(null);
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void isStopRequested_returns_false() {
        assertThat(createObjectUnderTest().isStopRequested(), equalTo(false));
    }

    @Test
    void isForceStopReadingBuffers_returns_false() {
        assertThat(createObjectUnderTest().isForceStopReadingBuffers(), equalTo(false));
    }

    @Test
    void isStopRequested_returns_true_after_shutdown() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();
        when(clock.millis()).thenReturn(Clock.systemUTC().millis());
        objectUnderTest.shutdown(dataPrepperShutdownOptions);
        assertThat(objectUnderTest.isStopRequested(), equalTo(true));
    }

    @Test
    void isStopRequested_returns_true_after_multiple_shutdown_calls() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();
        when(clock.millis()).thenReturn(Clock.systemUTC().millis());
        for (int i = 0; i < 10; i++) {
            objectUnderTest.shutdown(dataPrepperShutdownOptions);
        }
        assertThat(objectUnderTest.isStopRequested(), equalTo(true));
    }

    @Test
    void isForceStopReadingBuffers_returns_false_after_shutdown_if_getBufferReadTimeout_is_null() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();

        when(dataPrepperShutdownOptions.getBufferReadTimeout()).thenReturn(null);
        objectUnderTest.shutdown(dataPrepperShutdownOptions);

        assertThat(objectUnderTest.isForceStopReadingBuffers(), equalTo(false));
    }

    @Test
    void isForceStopReadingBuffers_returns_false_after_shutdown_if_time_is_before_shutdown_plus_getBufferReadTimeout() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();

        when(dataPrepperShutdownOptions.getBufferReadTimeout()).thenReturn(Duration.ofSeconds(1));
        final Instant baseTime = Instant.now();
        when(clock.millis())
                .thenReturn(baseTime.toEpochMilli());

        objectUnderTest.shutdown(dataPrepperShutdownOptions);

        assertThat(objectUnderTest.isForceStopReadingBuffers(), equalTo(false));
    }

    @Test
    void isForceStopReadingBuffers_returns_true_after_shutdown_if_time_is_after_shutdown_plus_getBufferReadTimeout() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();

        when(dataPrepperShutdownOptions.getBufferReadTimeout()).thenReturn(Duration.ofSeconds(1));
        final Instant baseTime = Instant.now();
        when(clock.millis())
                .thenReturn(baseTime.toEpochMilli())
                .thenReturn(baseTime.plusSeconds(2).toEpochMilli());

        objectUnderTest.shutdown(dataPrepperShutdownOptions);

        assertThat(objectUnderTest.isForceStopReadingBuffers(), equalTo(true));
    }

    @Test
    void isForceStopReadingBuffers_returns_true_if_shutdown_is_called_multiple_times() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();

        when(dataPrepperShutdownOptions.getBufferReadTimeout())
                .thenReturn(Duration.ofSeconds(1))
                .thenReturn(Duration.ofSeconds(5));
        final Instant baseTime = Instant.now();
        when(clock.millis())
                .thenReturn(baseTime.toEpochMilli())
                .thenReturn(baseTime.plusSeconds(2).toEpochMilli());

        objectUnderTest.shutdown(dataPrepperShutdownOptions);
        objectUnderTest.shutdown(dataPrepperShutdownOptions);

        assertThat(objectUnderTest.isForceStopReadingBuffers(), equalTo(true));
    }

    @Test
    void isForceStopReadingBuffers_returns_true_if_shutdown_is_called_in_between_isForceStopReadingBuffers_calls() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();

        when(dataPrepperShutdownOptions.getBufferReadTimeout())
                .thenReturn(Duration.ofSeconds(1))
                .thenReturn(Duration.ofSeconds(5));
        final Instant baseTime = Instant.now();
        when(clock.millis())
                .thenReturn(baseTime.toEpochMilli())
                .thenReturn(baseTime.plusSeconds(2).toEpochMilli());

        objectUnderTest.shutdown(dataPrepperShutdownOptions);
        assertThat(objectUnderTest.isForceStopReadingBuffers(), equalTo(true));

        objectUnderTest.shutdown(dataPrepperShutdownOptions);
        assertThat(objectUnderTest.isForceStopReadingBuffers(), equalTo(true));
    }


    @Test
    void getBufferDrainTimeout_returns_buffer_getDrainTimeout_if_shutdown_not_called() {
        assertThat(createObjectUnderTest().getBufferDrainTimeout(), equalTo(bufferDrainTimeout));
    }

    @Test
    void getBufferDrainTimeout_returns_buffer_getDrainTimeout_if_shutdown_called_without_bufferDrainTimeout() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();
        when(dataPrepperShutdownOptions.getBufferDrainTimeout()).thenReturn(null);
        objectUnderTest.shutdown(dataPrepperShutdownOptions);
        assertThat(objectUnderTest.getBufferDrainTimeout(), equalTo(bufferDrainTimeout));
    }

    @Test
    void getBufferDrainTimeout_returns_buffer_shutdownOptions_bufferDrainTimeout_if_provided() {
        final PipelineShutdown objectUnderTest = createObjectUnderTest();
        Duration bufferDrainTimeoutFromOptions = Duration.ofSeconds(random.nextInt(100) + 100);
        when(dataPrepperShutdownOptions.getBufferDrainTimeout()).thenReturn(bufferDrainTimeoutFromOptions);
        objectUnderTest.shutdown(dataPrepperShutdownOptions);
        assertThat(objectUnderTest.getBufferDrainTimeout(), equalTo(bufferDrainTimeoutFromOptions));
    }
}