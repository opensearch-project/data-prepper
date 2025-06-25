package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.DynamoDBSourceConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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

    private ShardAcknowledgementManager shardAcknowledgementManager;

    @BeforeEach
    void setUp() {
        when(dynamoDBSourceConfig.getShardAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(15));
        shardAcknowledgementManager = new ShardAcknowledgementManager(
            acknowledgementSetManager, sourceCoordinator, dynamoDBSourceConfig);
    }

    @Test
    void testCreateAcknowledgmentSet() {
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
            .thenReturn(acknowledgementSet);

        AcknowledgementSet result = shardAcknowledgementManager.createAcknowledgmentSet(
            streamPartition, "seq123", false);

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
    void testShutdown() {
        assertDoesNotThrow(() -> shardAcknowledgementManager.shutdown());
    }
}