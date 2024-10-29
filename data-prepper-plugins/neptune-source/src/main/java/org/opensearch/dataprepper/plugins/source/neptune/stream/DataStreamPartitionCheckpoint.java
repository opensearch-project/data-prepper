package org.opensearch.dataprepper.plugins.source.neptune.stream;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.neptune.model.StreamLoadStatus;
import org.opensearch.dataprepper.plugins.source.neptune.s3partition.S3FolderPartitionCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.opensearch.dataprepper.model.source.s3.S3ScanEnvironmentVariables.STOP_S3_SCAN_PROCESSING_PROPERTY;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.STREAM_PREFIX;

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
        super(enhancedSourceCoordinator);
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.streamPartition = streamPartition;
    }

    private void setProgressState(final Long commitNum, final Long opNum, final Long recordNumber) {
        //Always has a state.
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        progressState.get().setCommitNum(commitNum);
        progressState.get().setOpNum(opNum);
        progressState.get().setLoadedRecords(recordNumber);
        progressState.get().setLastUpdateTimestamp(Instant.now().toEpochMilli());
    }

    /**
     * This method is to do a checkpoint with latest resume token processed.
     * Note that this should be called on a regular basis even there are no changes to resume token
     * As the checkpoint will also extend the timeout for the lease
     *
     * @param commitNum   The commit number of the starting record to read from Neptune change-log stream
     * @param opNum       The operation sequence number within the specified commit to start reading from in Neptune change-log stream data
     * @param recordCount The last processed record count
     */
    public void checkpoint(final long commitNum, final long opNum, final long recordCount) {
        LOG.debug("Checkpoint stream partition with record number {}", recordCount);
        setProgressState(commitNum, opNum, recordCount);
        enhancedSourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    public void extendLease() {
        LOG.debug("Extending lease of stream partition");
        enhancedSourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    /**
     * This method is to reset checkpoint when change stream is invalid. The current thread will give up partition and new thread
     * will take ownership of partition. If change stream is valid then new thread proceeds with processing change stream else the
     * process repeats.
     */
    public void resetCheckpoint() {
        LOG.debug("Resetting checkpoint stream partition");
        setProgressState(0L, 0L, 0L);
        enhancedSourceCoordinator.giveUpPartition(streamPartition);
        System.clearProperty(STOP_S3_SCAN_PROCESSING_PROPERTY);
    }

    public Optional<StreamLoadStatus> getGlobalStreamLoadStatus() {
        final Optional<EnhancedSourcePartition> partition = enhancedSourceCoordinator.getPartition(STREAM_PREFIX + streamPartition.getPartitionKey());
        if (partition.isPresent()) {
            final GlobalState globalState = (GlobalState) partition.get();
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
        System.clearProperty(STOP_S3_SCAN_PROCESSING_PROPERTY);
    }
}
