/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.DataQueryProgressState;
import org.opensearch.dataprepper.plugins.mongo.s3partition.S3FolderPartitionCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * A helper class to handle the data query partition status and the progress state
 * It will use coordinator APIs under the hood.
 */
public class DataQueryPartitionCheckpoint extends S3FolderPartitionCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(DataQueryPartitionCheckpoint.class);

    static final Duration CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE = Duration.ofMinutes(5);


    private final EnhancedSourceCoordinator enhancedSourceCoordinator;

    private final DataQueryPartition dataQueryPartition;


    public DataQueryPartitionCheckpoint(EnhancedSourceCoordinator enhancedSourceCoordinator, DataQueryPartition dataQueryPartition) {
        super(enhancedSourceCoordinator, dataQueryPartition.getCollection());
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.dataQueryPartition = dataQueryPartition;
    }

    private void setProgressState(long records) {
        //Always has a state.
        Optional<DataQueryProgressState> progressState = dataQueryPartition.getProgressState();
        progressState.get().setLoadedRecords(records);
    }

    /**
     * This method is to do a checkpoint with latest sequence number processed.
     * Note that this should be called on a regular basis even there are no changes to sequence number
     * As the checkpoint will also extend the timeout for the lease
     *
     * @param recordNumber The last record number
     */
    public void checkpoint(int recordNumber) {
        LOG.debug("Checkpoint partition query " + dataQueryPartition.getQuery() + " with record number " + recordNumber);
        setProgressState(recordNumber);
        enhancedSourceCoordinator.saveProgressStateForPartition(dataQueryPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    public void updateDatafileForAcknowledgmentWait(final Duration acknowledgmentSetTimeout) {
        enhancedSourceCoordinator.saveProgressStateForPartition(dataQueryPartition, acknowledgmentSetTimeout);
    }
}
