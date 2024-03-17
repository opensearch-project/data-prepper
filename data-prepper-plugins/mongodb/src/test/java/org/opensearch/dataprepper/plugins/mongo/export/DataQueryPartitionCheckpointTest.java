package org.opensearch.dataprepper.plugins.mongo.export;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.DataQueryProgressState;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.export.DataQueryPartitionCheckpoint.CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE;

@ExtendWith(MockitoExtension.class)
public class DataQueryPartitionCheckpointTest {
    @Mock
    private EnhancedSourceCoordinator enhancedSourceCoordinator;

    @Mock
    private DataQueryPartition dataQueryPartition;

    @Mock
    private DataQueryProgressState dataQueryProgressState;

    @InjectMocks
    private DataQueryPartitionCheckpoint dataQueryPartitionCheckpoint;

    @Test
    public void checkpoint() {
        final int loadedRecords = new Random().nextInt();
        when(dataQueryPartition.getProgressState()).thenReturn(Optional.of(dataQueryProgressState));
        dataQueryPartitionCheckpoint.checkpoint(loadedRecords);
        verify(enhancedSourceCoordinator).saveProgressStateForPartition(dataQueryPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        verify(dataQueryProgressState).setLoadedRecords(loadedRecords);
    }

    @Test
    public void updateDatafileForAcknowledgmentWait() {
        final int minutes = new Random().nextInt();
        final Duration duration = Duration.ofMinutes(minutes);
        dataQueryPartitionCheckpoint.updateDatafileForAcknowledgmentWait(duration);
        verify(enhancedSourceCoordinator).saveProgressStateForPartition(dataQueryPartition, duration);
    }
}
