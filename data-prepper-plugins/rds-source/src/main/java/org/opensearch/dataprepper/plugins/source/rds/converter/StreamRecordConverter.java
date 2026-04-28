/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Convert binlog row data into JacksonEvent.
 * Uses a monotonically increasing version based on timestamp_millis * 1000 + sequence,
 * ensuring unique versions even for events within the same millisecond.
 */
public class StreamRecordConverter extends RecordConverter {

    private final AtomicLong versionCounter = new AtomicLong(0);

    public StreamRecordConverter(final String s3Prefix, final int partitionCount) {
        super(s3Prefix, partitionCount);
    }

    @Override
    String getIngestionType() {
        return STREAM_INGESTION_TYPE;
    }

    /**
     * Generates a monotonically increasing version number.
     * Uses timestamp_millis * 1000 as the base, with a sequence suffix.
     * If the clock moves forward, the counter resets to the new base.
     * If multiple events share the same millisecond, the counter increments.
     */
    public long getVersionNumber(final long eventTimestampMillis) {
        final long base = eventTimestampMillis * 1000;
        return versionCounter.updateAndGet(current -> Math.max(current + 1, base));
    }
}
