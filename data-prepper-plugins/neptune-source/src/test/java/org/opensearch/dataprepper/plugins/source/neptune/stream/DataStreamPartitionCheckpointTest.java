package org.opensearch.dataprepper.plugins.source.neptune.stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamCheckpoint;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamPosition;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalToObject;
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

        final StreamCheckpoint checkpoint = new StreamCheckpoint(new StreamPosition(commitNum, opNum), recordNumber);
        dataStreamPartitionCheckpoint.checkpoint(checkpoint);

        verify(enhancedSourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        verify(streamProgressState).updateFromCheckpoint(checkpoint);
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

        final ArgumentCaptor<StreamCheckpoint> argumentCaptor = ArgumentCaptor.forClass(StreamCheckpoint.class);
        verify(streamProgressState).updateFromCheckpoint(argumentCaptor.capture());

        final StreamCheckpoint checkpoint = argumentCaptor.getValue();
        assertThat(checkpoint, equalToObject(StreamCheckpoint.emptyProgress()));
    }

    @Test
    public void extendLease_success() {
        dataStreamPartitionCheckpoint.extendLease();
        verify(enhancedSourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }
}
