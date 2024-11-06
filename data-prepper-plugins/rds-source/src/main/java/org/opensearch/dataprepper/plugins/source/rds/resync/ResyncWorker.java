/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;
import org.opensearch.dataprepper.plugins.source.rds.schema.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class ResyncWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ResyncWorker.class);

    static final String DATA_PREPPER_EVENT_TYPE = "event";
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final ResyncPartition resyncPartition;
    private final RdsSourceConfig sourceConfig;
    private final QueryManager queryManager;
    private final Buffer<Record<Event>> buffer;
    private final RecordConverter recordConverter;
    private final AcknowledgementSet acknowledgementSet;

    ResyncWorker(ResyncPartition resyncPartition,
                 RdsSourceConfig sourceConfig,
                 QueryManager queryManager,
                 Buffer<Record<Event>> buffer,
                 RecordConverter recordConverter,
                 AcknowledgementSet acknowledgementSet) {
        this.resyncPartition = resyncPartition;
        this.sourceConfig = sourceConfig;
        this.queryManager = queryManager;
        this.buffer = buffer;
        this.recordConverter = recordConverter;
        this.acknowledgementSet = acknowledgementSet;
    }

    public static ResyncWorker create(ResyncPartition resyncPartition,
                                      RdsSourceConfig sourceConfig,
                                      QueryManager queryManager,
                                      Buffer<Record<Event>> buffer,
                                      RecordConverter recordConverter,
                                      AcknowledgementSet acknowledgementSet) {
        return new ResyncWorker(resyncPartition, sourceConfig, queryManager, buffer, recordConverter, acknowledgementSet);
    }

    public void run() {
        String[] keySplits = resyncPartition.getPartitionKey().split("\\|");
        final String database = keySplits[0];
        final String table = keySplits[1];
        final long eventTimestampMillis = Long.parseLong(keySplits[2]);

        if (resyncPartition.getProgressState().isEmpty()) {
            final String errorMessage = "ResyncPartition " + resyncPartition.getPartitionKey() + " doesn't contain progress state.";
            throw new RuntimeException(errorMessage);
        }
        final ResyncProgressState progressState = resyncPartition.getProgressState().get();
        final String foreignKeyName = progressState.getForeignKeyName();
        final Object updatedValue = progressState.getUpdatedValue();

        LOG.debug("Will perform resync on table: {}.{}, with foreign key name: {}, and updated value: {}", database, table, foreignKeyName, updatedValue);
        String queryStatement;
        if (updatedValue == null) {
            queryStatement = String.format("SELECT * FROM %s WHERE %s IS NULL", database + "." + table, foreignKeyName);
        } else {
            queryStatement = String.format("SELECT * FROM %s WHERE %s='%s'", database + "." + table, foreignKeyName, updatedValue);
        }
        LOG.debug("Query statement: {}", queryStatement);

        List<Map<String, Object>> rows = queryManager.selectRows(queryStatement);
        LOG.debug("Found {} rows to resync", rows.size());

        BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);

        for (Map<String, Object> row : rows) {
            final Event dataPrepperEvent = JacksonEvent.builder()
                    .withEventType(DATA_PREPPER_EVENT_TYPE)
                    .withData(row)
                    .build();

            final Event pipelineEvent = recordConverter.convert(
                    dataPrepperEvent,
                    database,
                    table,
                    OpenSearchBulkActions.INDEX,
                    progressState.getPrimaryKeys(),
                    eventTimestampMillis,
                    eventTimestampMillis,
                    null);

            if (acknowledgementSet != null) {
                acknowledgementSet.add(pipelineEvent);
            }

            try {
                bufferAccumulator.add(new Record<>(pipelineEvent));
            } catch (Exception e) {
                LOG.error("Failed to add event to buffer", e);
                throw new RuntimeException(e);
            }
        }

        try {
            bufferAccumulator.flush();
            if (acknowledgementSet != null) {
                acknowledgementSet.complete();
            }
        } catch (Exception e) {
            // this will only happen if writing to buffer gets interrupted from shutdown,
            // otherwise bufferAccumulator will keep retrying with backoff
            LOG.error("Failed to flush buffer", e);
        }
    }
}
