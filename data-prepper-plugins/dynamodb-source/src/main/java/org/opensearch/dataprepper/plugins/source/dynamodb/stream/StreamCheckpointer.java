/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * A helper class to handle the stream partition status and the progress state
 * It will use coordinator APIs under the hood.
 */
public class StreamCheckpointer {
    private static final Logger LOG = LoggerFactory.getLogger(StreamCheckpointer.class);

    private final EnhancedSourceCoordinator coordinator;

    private final StreamPartition streamPartition;

    public StreamCheckpointer(EnhancedSourceCoordinator coordinator, StreamPartition streamPartition) {
        this.coordinator = coordinator;
        this.streamPartition = streamPartition;
    }

    private void setSequenceNumber(String sequenceNumber) {
        // Must only update progress if sequence number is not empty
        // A blank sequence number means the current sequence number in the progress state has not changed, do nothing
        if (sequenceNumber != null && !sequenceNumber.isEmpty()) {
            Optional<StreamProgressState> progressState = streamPartition.getProgressState();
            if (progressState.isPresent()) {
                progressState.get().setSequenceNumber(sequenceNumber);
            }
        }
    }

    /**
     * This method is to do a checkpoint with latest sequence number processed.
     * Note that this should be called on a regular basis even there are no changes to sequence number
     * As the checkpoint will also extend the timeout for the lease
     *
     * @param sequenceNumber The last sequence number
     */

    public void checkpoint(String sequenceNumber) {
        LOG.debug("Checkpoint shard " + streamPartition.getShardId() + " with sequenceNumber " + sequenceNumber);
        setSequenceNumber(sequenceNumber);
        coordinator.saveProgressStateForPartition(streamPartition);

    }

    /**
     * This method is to mark the shard partition as COMPLETED with the final sequence number
     * Note that this should be called when reaching the end of shard.
     *
     * @param sequenceNumber The last sequence number
     */

    public void complete(String sequenceNumber) {
        LOG.debug("Complete the read of shard " + streamPartition.getShardId() + " with final sequenceNumber " + sequenceNumber);
        setSequenceNumber(sequenceNumber);
        coordinator.completePartition(streamPartition);

    }

    /**
     * This method is to release the lease of the stream partition.
     * Normally this should only be called due to failures or interruption.
     *
     * @param sequenceNumber The last sequence number
     */
    public void release(String sequenceNumber) {
        LOG.debug("Release the ownership of shard " + streamPartition.getShardId() + " with final sequenceNumber " + sequenceNumber);
        setSequenceNumber(sequenceNumber);
        coordinator.giveUpPartition(streamPartition);

    }

    public boolean isExportDone() {
        Optional<EnhancedSourcePartition> globalPartition = coordinator.getPartition(streamPartition.getStreamArn());
        return globalPartition.isPresent();
    }

}
