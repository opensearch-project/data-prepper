/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for SynchronizedBuffer.
 */
public class SynchronizedBufferConfig {
    public static final int DEFAULT_BATCH_SIZE = 2;

    @JsonProperty("batch_size")
    private int batchSize = DEFAULT_BATCH_SIZE;

    /**
     * Gets the batch size for reading records from the buffer.
     *
     * @return The batch size
     */
    public int getBatchSize() {
        return batchSize;
    }
}
