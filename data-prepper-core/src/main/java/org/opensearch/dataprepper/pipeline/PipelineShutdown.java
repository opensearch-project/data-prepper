/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import org.opensearch.dataprepper.DataPrepperShutdownOptions;
import org.opensearch.dataprepper.model.buffer.Buffer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

class PipelineShutdown {
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final Duration bufferDrainTimeout;
    private final Clock clock;
    private Instant shutdownRequestedAt;
    private Instant forceStopReadingBuffersAt;
    private Duration bufferDrainTimeoutOverride;

    PipelineShutdown(final Buffer<?> buffer) {
        this(buffer, Clock.systemDefaultZone());
    }

    PipelineShutdown(final Buffer<?> buffer, final Clock clock) {
        bufferDrainTimeout = Objects.requireNonNull(buffer.getDrainTimeout());
        this.clock = clock;
    }

    public void shutdown(final DataPrepperShutdownOptions dataPrepperShutdownOptions) {
        final boolean stopPreviouslyRequested = stopRequested.get();
        if(stopPreviouslyRequested) {
            return;
        }

        stopRequested.set(true);
        shutdownRequestedAt = now();

        final Duration bufferReadTimeout = dataPrepperShutdownOptions.getBufferReadTimeout();
        if(bufferReadTimeout != null) {
            forceStopReadingBuffersAt = shutdownRequestedAt.plus(bufferReadTimeout);
        }

        final Duration bufferDrainTimeoutOverride = dataPrepperShutdownOptions.getBufferDrainTimeout();
        if(bufferDrainTimeoutOverride != null) {
            this.bufferDrainTimeoutOverride = bufferDrainTimeoutOverride;
        }
    }

    boolean isStopRequested() {
        return stopRequested.get();
    }

    boolean isForceStopReadingBuffers() {
        return forceStopReadingBuffersAt != null && now().isAfter(forceStopReadingBuffersAt);
    }

    public Duration getBufferDrainTimeout() {
        return bufferDrainTimeoutOverride != null ?
                bufferDrainTimeoutOverride : bufferDrainTimeout;
    }

    private Instant now() {
        return Instant.ofEpochMilli(clock.millis());
    }
}
