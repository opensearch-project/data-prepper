/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

/**
 * Convert binlog row data into JacksonEvent
 */
public class StreamRecordConverter extends RecordConverter {

    public StreamRecordConverter(final String s3Prefix, final int partitionCount) {
        super(s3Prefix, partitionCount);
    }

    @Override
    String getIngestionType() {
        return STREAM_INGESTION_TYPE;
    }
}
