/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.buffer.blockingbuffer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BlockingBufferConfig {
    public static final int DEFAULT_BUFFER_CAPACITY = 12_800;
    public static final int DEFAULT_BATCH_SIZE = 200;


    @JsonProperty("buffer_size")
    private int bufferSize = DEFAULT_BUFFER_CAPACITY;

    public int getBufferSize() {
        return bufferSize;
    }

    @JsonProperty("batch_size")
    private int batchSize = DEFAULT_BATCH_SIZE;

    public int getBatchSize() {
        return batchSize;
    }
}
