/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

public class BatchActionExecutor implements MLActionExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(BatchActionExecutor.class);

    private final MLBatchJobCreator mlBatchJobCreator;

    public BatchActionExecutor(final MLBatchJobCreator mlBatchJobCreator) {
        this.mlBatchJobCreator = mlBatchJobCreator;
    }

    @Override
    public void prepareExecution(final List<Record<Event>> resultRecords) {
        mlBatchJobCreator.checkAndProcessBatch();
        mlBatchJobCreator.addProcessedBatchRecordsToResults(resultRecords);
    }

    @Override
    public Collection<Record<Event>> execute(final List<Record<Event>> filteredRecords,
                                              final List<Record<Event>> resultRecords) {
        try {
            mlBatchJobCreator.createMLBatchJob(filteredRecords, resultRecords);
        } catch (final MLBatchJobException e) {
            LOG.error(NOISY, "ML Batch job creation failed: {}", e.getMessage());
            throw e;
        }
        return resultRecords;
    }

    @Override
    public void prepareForShutdown() {
        mlBatchJobCreator.prepareForShutdown();
    }

    @Override
    public boolean isReadyForShutdown() {
        return mlBatchJobCreator.isReadyForShutdown();
    }

    @Override
    public void shutdown() {
        mlBatchJobCreator.shutdown();
    }
}
