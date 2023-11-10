/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * A helper class to handle the data file partition status and the progress state
 * It will use coordinator APIs under the hood.
 */
public class DataFileCheckpointer {
    private static final Logger LOG = LoggerFactory.getLogger(DataFileCheckpointer.class);

    static final Duration CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE = Duration.ofMinutes(5);


    private final EnhancedSourceCoordinator enhancedSourceCoordinator;

    private final DataFilePartition dataFilePartition;


    public DataFileCheckpointer(EnhancedSourceCoordinator enhancedSourceCoordinator, DataFilePartition dataFilePartition) {
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.dataFilePartition = dataFilePartition;
    }

    private void setProgressState(int lineNumber) {
        //Always has a state.
        Optional<DataFileProgressState> progressState = dataFilePartition.getProgressState();
        progressState.get().setLoaded(lineNumber);
    }

    /**
     * This method is to do a checkpoint with latest sequence number processed.
     * Note that this should be called on a regular basis even there are no changes to sequence number
     * As the checkpoint will also extend the timeout for the lease
     *
     * @param lineNumber The last line number
     */
    public void checkpoint(int lineNumber) {
        LOG.debug("Checkpoint data file " + dataFilePartition.getKey() + " with line number " + lineNumber);
        setProgressState(lineNumber);
        enhancedSourceCoordinator.saveProgressStateForPartition(dataFilePartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    /**
     * This method is to mark the shard partition as COMPLETED with the final sequence number
     * Note that this should be called when reaching the end of shard.
     *
     * @param lineNumber The last line number
     */
    public void complete(int lineNumber) {
        LOG.debug("Complete the read of data file " + dataFilePartition.getKey() + " with line number " + lineNumber);
        setProgressState(lineNumber);
        enhancedSourceCoordinator.completePartition(dataFilePartition);
    }

    /**
     * This method is to release the lease of the data file partition.
     * Normally this should only be called due to failures or interruption.
     *
     * @param lineNumber The last line number
     */
    public void release(int lineNumber) {
        LOG.debug("Release the ownership of data file " + dataFilePartition.getKey() + " with line number " + lineNumber);
        setProgressState(lineNumber);
        enhancedSourceCoordinator.giveUpPartition(dataFilePartition);
    }

    public void updateDatafileForAcknowledgmentWait(final Duration acknowledgmentSetTimeout) {
        enhancedSourceCoordinator.saveProgressStateForPartition(dataFilePartition, acknowledgmentSetTimeout);
    }

}
