/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.DynamoDBSourceConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import static org.mockito.Mockito.doThrow;

import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class ShardAcknowledgementManagerTest {

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private DynamoDBSourceConfig dynamoDBSourceConfig;

    @Mock
    private StreamPartition streamPartition;

    @Mock
    private StreamProgressState streamProgressState;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Consumer<StreamPartition> stopWorkerConsumer;

    @Captor
    private ArgumentCaptor<Consumer<Boolean>> ackCallbackCaptor;

    private ShardAcknowledgementManager shardAcknowledgementManager;

    @BeforeEach
    void setUp() {
        shardAcknowledgementManager = new ShardAcknowledgementManager(
            acknowledgementSetManager, sourceCoordinator, dynamoDBSourceConfig, stopWorkerConsumer);
    }

    @Test
    void testCreateAcknowledgmentSet() {
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
            .thenReturn(acknowledgementSet);

        AcknowledgementSet result = shardAcknowledgementManager.createAcknowledgmentSet(
            streamPartition, "seq123", false);

        assertNotNull(result);
        verify(acknowledgementSetManager).create(any(Consumer.class), eq(Duration.ofMinutes(15)));
    }

    @Test
    void testCreateAcknowledgmentSet_withNullSequenceNumber() {
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
            .thenReturn(acknowledgementSet);

        AcknowledgementSet result = shardAcknowledgementManager.createAcknowledgmentSet(
            streamPartition, null, false);

        assertNotNull(result);
        verify(acknowledgementSetManager).create(any(Consumer.class), eq(Duration.ofMinutes(15)));
    }

    @Test
    void testIsExportDone() {
        when(streamPartition.getStreamArn()).thenReturn("stream-arn");
        when(sourceCoordinator.getPartition("stream-arn")).thenReturn(Optional.of(streamPartition));

        boolean result = shardAcknowledgementManager.isExportDone(streamPartition);

        assertTrue(result);
    }

    @Test
    void testIsExportDone_whenPartitionNotPresent() {
        when(streamPartition.getStreamArn()).thenReturn("stream-arn");
        when(sourceCoordinator.getPartition("stream-arn")).thenReturn(Optional.empty());

        boolean result = shardAcknowledgementManager.isExportDone(streamPartition);

        assertFalse(result);
    }

    @Test
    void testStartUpdatingOwnershipForShard() {
        shardAcknowledgementManager.startUpdatingOwnershipForShard(streamPartition);

        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
            .thenReturn(acknowledgementSet);

        AcknowledgementSet result = shardAcknowledgementManager.createAcknowledgmentSet(
            streamPartition, "seq123", false);

        assertNotNull(result);
    }

    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> shardAcknowledgementManager.shutdown());
    }

    @Test
    void testPositiveAcknowledgment_successfullyCompletesCheckpoint() {

        // Setup
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        when(acknowledgementSetManager.create(ackCallbackCaptor.capture(), any(Duration.class)))
            .thenReturn(acknowledgementSet);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        // Create acknowledgment set and start updating ownership
        shardAcknowledgementManager.startUpdatingOwnershipForShard(streamPartition);
        shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq123", false);

        // Simulate positive acknowledgment
        ackCallbackCaptor.getValue().accept(true);

        // Run the acknowledgment loop
        shardAcknowledgementManager.runMonitorAcknowledgmentLoop(stopWorkerConsumer);

        // Verify the checkpoint was saved with positive acknowledgment
        verify(streamProgressState).setSequenceNumber("seq123");
        verify(sourceCoordinator).saveProgressStateForPartition(eq(streamPartition), any(Duration.class));
    }

    @Test
    void testPositiveAcknowledgment_finalAcknowledgmentSet_completes_partition() {

        // Setup
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        when(acknowledgementSetManager.create(ackCallbackCaptor.capture(), any(Duration.class)))
                .thenReturn(acknowledgementSet);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        // Create acknowledgment set and start updating ownership
        shardAcknowledgementManager.startUpdatingOwnershipForShard(streamPartition);
        shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq123", true);

        // Simulate positive acknowledgment
        ackCallbackCaptor.getValue().accept(true);

        // Run the acknowledgment loop
        shardAcknowledgementManager.runMonitorAcknowledgmentLoop(stopWorkerConsumer);

        verify(sourceCoordinator).completePartition(streamPartition);
    }

    @Test
    void testAcknowledgmentTimeout() throws Exception {
        // Setup
        Duration timeout = Duration.ofMillis(1);
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(timeout);
        when(acknowledgementSetManager.create(ackCallbackCaptor.capture(), any(Duration.class)))
            .thenReturn(acknowledgementSet);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        // Create acknowledgment set and start updating ownership
        shardAcknowledgementManager.startUpdatingOwnershipForShard(streamPartition);
        shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq123", false);

        Thread.sleep(10);

        // Run the acknowledgment loop
        shardAcknowledgementManager.runMonitorAcknowledgmentLoop(stopWorkerConsumer);

        verify(sourceCoordinator).giveUpPartition(streamPartition);
        verifyNoInteractions(streamProgressState);
    }

    @Test
    void testMultipleAcknowledgments_handledCorrectly() {
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        // Setup
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        when(acknowledgementSetManager.create(ackCallbackCaptor.capture(), any(Duration.class)))
            .thenReturn(acknowledgementSet);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        // Create first acknowledgment set and capture its callback
        shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq123", false);
        Consumer<Boolean> callback1 = ackCallbackCaptor.getValue();

        // Create second acknowledgment set and capture its callback
        shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq124", false);
        Consumer<Boolean> callback2 = ackCallbackCaptor.getValue();

        // Simulate mixed acknowledgments
        callback1.accept(true);  // Positive ack
        callback2.accept(false); // Negative ack

        // Run the acknowledgment loop
        shardAcknowledgementManager.runMonitorAcknowledgmentLoop(stopWorkerConsumer);

        final InOrder inOrder = Mockito.inOrder(sourceCoordinator, streamProgressState);
        inOrder.verify(streamProgressState).setSequenceNumber("seq123");
        inOrder.verify(sourceCoordinator).saveProgressStateForPartition(streamPartition, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
        inOrder.verify(sourceCoordinator).giveUpPartition(streamPartition);

        verifyNoMoreInteractions(streamProgressState);
    }

    /*
    @Test
    void create_acknowledgment_set_throws_ShardNotTrackedException_if_shard_is_not_being_tracked() {
        assertThrows(ShardNotTrackedException.class, () -> shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq123", false));
    }


     */
    @Test
    void failure_to_update_state_for_one_partition_stops_tracking_that_partition_and_continues_on_others() {
        final StreamPartition secondPartition = mock(StreamPartition.class);


        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        when(secondPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        // Setup
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        when(acknowledgementSetManager.create(ackCallbackCaptor.capture(), any(Duration.class)))
                .thenReturn(acknowledgementSet);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));

        shardAcknowledgementManager.startUpdatingOwnershipForShard(streamPartition);
        // Create first acknowledgment set and capture its callback
        shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq123", false);
        Consumer<Boolean> callback1 = ackCallbackCaptor.getValue();

        // Create second acknowledgment set and capture its callback
        shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, "seq124", false);
        Consumer<Boolean> callback2 = ackCallbackCaptor.getValue();

        shardAcknowledgementManager.startUpdatingOwnershipForShard(secondPartition);
        // Create first acknowledgment set and capture its callback
        shardAcknowledgementManager.createAcknowledgmentSet(secondPartition, "seq123", false);
        Consumer<Boolean> callback1_2 = ackCallbackCaptor.getValue();

        // Create second acknowledgment set and capture its callback
        shardAcknowledgementManager.createAcknowledgmentSet(secondPartition, "seq124", false);
        Consumer<Boolean> callback2_2 = ackCallbackCaptor.getValue();

        // Simulate mixed acknowledgments
        callback1.accept(true);  // Positive ack
        callback2.accept(true);

        callback1_2.accept(true);
        callback2_2.accept(true);

        doThrow(PartitionUpdateException.class).when(sourceCoordinator).saveProgressStateForPartition(eq(streamPartition),
                any(Duration.class));


        // Run the acknowledgment loop
        //assertThat(shardAcknowledgementManager.isStillTrackingShard(streamPartition), equalTo(true));
        //assertThat(shardAcknowledgementManager.isStillTrackingShard(secondPartition), equalTo(true));
        shardAcknowledgementManager.runMonitorAcknowledgmentLoop(stopWorkerConsumer);
        //assertThat(shardAcknowledgementManager.isStillTrackingShard(streamPartition), equalTo(false));
        //assertThat(shardAcknowledgementManager.isStillTrackingShard(secondPartition), equalTo(true));
        shardAcknowledgementManager.runMonitorAcknowledgmentLoop(stopWorkerConsumer);

        final InOrder inOrder = Mockito.inOrder(sourceCoordinator, streamProgressState);
        inOrder.verify(streamProgressState).setSequenceNumber("seq124");
        inOrder.verify(sourceCoordinator).saveProgressStateForPartition(secondPartition, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
        inOrder.verify(sourceCoordinator).giveUpPartition(streamPartition);
    }

}
