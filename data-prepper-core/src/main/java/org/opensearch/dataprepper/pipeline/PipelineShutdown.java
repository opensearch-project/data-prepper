/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import org.opensearch.dataprepper.DataPrepperShutdownOptions;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

class PipelineShutdown {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineShutdown.class);

    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final Duration pipelineConfiguredBufferDrainTimeout;
    private final Clock clock;
    private final String pipelineName;
    private Instant shutdownRequestedAt;
    private Instant forceStopReadingBuffersAt;
    private Duration bufferDrainTimeoutOverride;
    private Duration bufferDrainTimeout;

    PipelineShutdown(final String pipelineName, final Buffer<?> buffer) {
        this(pipelineName, buffer, Clock.systemDefaultZone());
    }

    PipelineShutdown(String pipelineName, final Buffer<?> buffer, final Clock clock) {
        this.pipelineName = pipelineName;
        pipelineConfiguredBufferDrainTimeout = Objects.requireNonNull(buffer.getDrainTimeout());
        bufferDrainTimeout = pipelineConfiguredBufferDrainTimeout;
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
            bufferDrainTimeout = bufferDrainTimeoutOverride;
        }

        LOG.info("Started shutdown for pipeline {}. Requested at {}. Force stop reading buffers at {}. The buffer drain timeout to use is {}",
                pipelineName, shutdownRequestedAt, forceStopReadingBuffersAt, bufferDrainTimeout);
    }

    boolean isStopRequested() {
        return stopRequested.get();
    }

    boolean isForceStopReadingBuffers() {
        return forceStopReadingBuffersAt != null && now().isAfter(forceStopReadingBuffersAt);
    }

    public Duration getBufferDrainTimeout() {
        return bufferDrainTimeout;
    }

    private Instant now() {
        return Instant.ofEpochMilli(clock.millis());
    }
}
