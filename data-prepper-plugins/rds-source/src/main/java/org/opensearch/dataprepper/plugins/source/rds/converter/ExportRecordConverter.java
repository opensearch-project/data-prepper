/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

public class ExportRecordConverter extends RecordConverter {

    public ExportRecordConverter(final String s3Prefix, final int partitionCount) {
        super(s3Prefix, partitionCount);
    }

    @Override
    String getIngestionType() {
        return EXPORT_INGESTION_TYPE;
    }
}
