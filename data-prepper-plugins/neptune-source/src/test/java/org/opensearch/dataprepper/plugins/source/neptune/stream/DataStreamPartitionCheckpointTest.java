package org.opensearch.dataprepper.plugins.source.neptune.stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.DataStreamPartitionCheckpoint.CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE;

@ExtendWith(MockitoExtension.class)
public class DataStreamPartitionCheckpointTest {
    @Mock
    private EnhancedSourceCoordinator enhancedSourceCoordinator;

    @Mock
    private StreamPartition streamPartition;

    @Mock
    private StreamProgressState streamProgressState;

    @InjectMocks
    private DataStreamPartitionCheckpoint dataStreamPartitionCheckpoint;

    @Test
    public void checkpoint_success() {
        final long commitNum = new Random().nextLong();
        final long opNum = new Random().nextLong();
        final long recordNumber = new Random().nextLong();
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        dataStreamPartitionCheckpoint.checkpoint(commitNum, opNum, recordNumber);

        verify(enhancedSourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        verify(streamProgressState).setCommitNum(commitNum);
        verify(streamProgressState).setOpNum(opNum);
        verify(streamProgressState).setLoadedRecords(recordNumber);
        verify(streamProgressState).setLastUpdateTimestamp(anyLong());
    }

    @Test
    public void updateStreamPartitionForAcknowledgmentWait_success() {
        final int minutes = new Random().nextInt();
        final Duration duration = Duration.ofMinutes(minutes);
        dataStreamPartitionCheckpoint.updateStreamPartitionForAcknowledgmentWait(duration);
        verify(enhancedSourceCoordinator).saveProgressStateForPartition(streamPartition, duration);
    }

    @Test
    public void resetCheckpoint_success() {
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        dataStreamPartitionCheckpoint.resetCheckpoint();
        verify(enhancedSourceCoordinator).giveUpPartition(streamPartition);
        verify(streamProgressState).setLoadedRecords(0L);
        verify(streamProgressState).setLastUpdateTimestamp(anyLong());
    }

    @Test
    public void extendLease_success() {
        dataStreamPartitionCheckpoint.extendLease();
        verify(enhancedSourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }
}
