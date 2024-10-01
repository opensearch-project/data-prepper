/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import java.time.Duration;

public class DataPrepperShutdownOptions {
    private final Duration bufferReadTimeout;
    private final Duration bufferDrainTimeout;

    public static DataPrepperShutdownOptions defaultOptions() {
        return new DataPrepperShutdownOptions(builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Duration bufferReadTimeout;
        private Duration bufferDrainTimeout;

        private Builder() {
        }

        public Builder withBufferReadTimeout(final Duration bufferReadTimeout) {
            this.bufferReadTimeout = bufferReadTimeout;
            return this;
        }

        public Builder withBufferDrainTimeout(final Duration bufferDrainTimeout) {
            this.bufferDrainTimeout = bufferDrainTimeout;
            return this;
        }

        public DataPrepperShutdownOptions build() {
            return new DataPrepperShutdownOptions(this);
        }
    }

    private DataPrepperShutdownOptions(final Builder builder) {
        this.bufferReadTimeout = builder.bufferReadTimeout;
        this.bufferDrainTimeout = builder.bufferDrainTimeout;

        if(bufferReadTimeout != null && bufferDrainTimeout != null) {
            if (bufferReadTimeout.compareTo(bufferDrainTimeout) > 0) {
                throw new IllegalArgumentException("Buffer read timeout cannot be greater than buffer drain timeout");
            }
        }
    }

    public Duration getBufferReadTimeout() {
        return bufferReadTimeout;
    }

    public Duration getBufferDrainTimeout() {
        return bufferDrainTimeout;
    }
}
