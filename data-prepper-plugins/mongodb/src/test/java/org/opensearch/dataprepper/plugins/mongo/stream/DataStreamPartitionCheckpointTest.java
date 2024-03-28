package org.opensearch.dataprepper.plugins.mongo.stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.stream.DataStreamPartitionCheckpoint.CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE;

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
    public void checkpoint() {
        final int recordNumber = new Random().nextInt();
        final String checkpointToken = UUID.randomUUID().toString();
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        dataStreamPartitionCheckpoint.checkpoint(checkpointToken, recordNumber);
        verify(enhancedSourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        verify(streamProgressState).setResumeToken(checkpointToken);
        verify(streamProgressState).setLoadedRecords(recordNumber);
        verify(streamProgressState).setLastUpdateTimestamp(anyLong());
    }

    @Test
    public void updateStreamPartitionForAcknowledgmentWait() {
        final int minutes = new Random().nextInt();
        final Duration duration = Duration.ofMinutes(minutes);
        dataStreamPartitionCheckpoint.updateStreamPartitionForAcknowledgmentWait(duration);
        verify(enhancedSourceCoordinator).saveProgressStateForPartition(streamPartition, duration);
    }
}
