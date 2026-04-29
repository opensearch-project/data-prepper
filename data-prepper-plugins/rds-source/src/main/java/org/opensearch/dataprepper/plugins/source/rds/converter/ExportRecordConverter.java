/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import java.util.concurrent.atomic.AtomicLong;

public class ExportRecordConverter extends RecordConverter {

    private final AtomicLong versionCounter = new AtomicLong(0);

    public ExportRecordConverter(final String s3Prefix, final int partitionCount) {
        super(s3Prefix, partitionCount);
    }

    @Override
    String getIngestionType() {
        return EXPORT_INGESTION_TYPE;
    }

    public long getVersionNumber(final long eventTimestampMillis) {
        final long base = eventTimestampMillis * 1000;
        return versionCounter.updateAndGet(current -> Math.max(current + 1, base));
    }
}
