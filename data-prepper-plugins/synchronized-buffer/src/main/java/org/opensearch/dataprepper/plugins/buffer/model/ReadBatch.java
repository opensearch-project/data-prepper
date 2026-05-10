/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer.model;

import org.opensearch.dataprepper.model.record.Record;

/**
 * Tracks a slice of records that a reader has consumed from a {@link SignaledBatch},
 * along with the count of records in that slice. Used during checkpointing to
 * mark the correct number of records as processed in the originating batch.
 */
public class ReadBatch<T extends Record<?>> {
    private final SignaledBatch<T> batch;
    private final int count;

    public ReadBatch(SignaledBatch<T> batch, int count) {
        this.batch = batch;
        this.count = count;
    }

    public SignaledBatch<T> getBatch() {
        return batch;
    }

    public int getCount() {
        return count;
    }
}
