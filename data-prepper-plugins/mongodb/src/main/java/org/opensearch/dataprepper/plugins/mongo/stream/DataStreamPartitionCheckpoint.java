/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.stream;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.mongo.model.StreamLoadStatus;
import org.opensearch.dataprepper.plugins.mongo.s3partition.S3FolderPartitionCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.STREAM_PREFIX;

/**
 * A helper class to handle the data query partition status and the progress state
 * It will use coordinator APIs under the hood.
 */
public class DataStreamPartitionCheckpoint extends S3FolderPartitionCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamPartitionCheckpoint.class);

    static final Duration CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE = Duration.ofMinutes(5);


    private final EnhancedSourceCoordinator enhancedSourceCoordinator;

    private final StreamPartition streamPartition;


    public DataStreamPartitionCheckpoint(final EnhancedSourceCoordinator enhancedSourceCoordinator,
                                         final StreamPartition streamPartition) {
        super(enhancedSourceCoordinator, streamPartition.getCollection());
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.streamPartition = streamPartition;
    }

    private void setProgressState(final String resumeToken, final long recordNumber) {
        //Always has a state.
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        progressState.get().setResumeToken(resumeToken);
        progressState.get().setLoadedRecords(recordNumber);
        progressState.get().setLastUpdateTimestamp(Instant.now().toEpochMilli());
    }

    /**
     * This method is to do a checkpoint with latest resume token processed.
     * Note that this should be called on a regular basis even there are no changes to resume token
     * As the checkpoint will also extend the timeout for the lease
     *
     * @param resumeToken checkpoint token to start resuming the stream when MongoDB/DocumentDB cursor is open
     * @param recordCount The last processed record count
     */
    public void checkpoint(final String resumeToken, final long recordCount) {
        LOG.debug("Checkpoint stream partition for collection " + streamPartition.getCollection() + " with record number " + recordCount);
        setProgressState(resumeToken, recordCount);
        enhancedSourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    public Optional<StreamLoadStatus> getGlobalStreamLoadStatus() {
        final Optional<EnhancedSourcePartition> partition = enhancedSourceCoordinator.getPartition(STREAM_PREFIX + streamPartition.getPartitionKey());
        if(partition.isPresent()) {
            final GlobalState globalState = (GlobalState)partition.get();
            return Optional.of(StreamLoadStatus.fromMap(globalState.getProgressState().get()));
        } else {
            return Optional.empty();
        }
    }

    public void updateStreamPartitionForAcknowledgmentWait(final Duration acknowledgmentSetTimeout) {
        enhancedSourceCoordinator.saveProgressStateForPartition(streamPartition, acknowledgmentSetTimeout);
    }

    public void giveUpPartition() {
        enhancedSourceCoordinator.giveUpPartition(streamPartition);
    }
}
