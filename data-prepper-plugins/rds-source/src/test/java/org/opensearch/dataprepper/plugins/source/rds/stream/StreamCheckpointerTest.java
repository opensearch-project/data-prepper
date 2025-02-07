/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.MySqlStreamState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.PostgresStreamState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.postgresql.replication.LogSequenceNumber;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.stream.StreamCheckpointer.CHECKPOINT_COUNT;
import static org.opensearch.dataprepper.plugins.source.rds.stream.StreamCheckpointer.CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE;


@ExtendWith(MockitoExtension.class)
class StreamCheckpointerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private StreamPartition streamPartition;

    @Mock
    private MySqlStreamState mySqlStreamState;

    @Mock
    private PostgresStreamState postgresStreamState;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter checkpointCounter;

    @Mock
    private ChangeEventStatus changeEventStatus;

    private StreamCheckpointer streamCheckpointer;


    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(CHECKPOINT_COUNT)).thenReturn(checkpointCounter);
        streamCheckpointer = createObjectUnderTest();
    }

    @Test
    void test_checkpoint_mysql() {
        final BinlogCoordinate binlogCoordinate = mock(BinlogCoordinate.class);
        final StreamProgressState streamProgressState = mock(StreamProgressState.class);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamProgressState.getMySqlStreamState()).thenReturn(mySqlStreamState);
        when(changeEventStatus.getBinlogCoordinate()).thenReturn(binlogCoordinate);

        streamCheckpointer.checkpoint(EngineType.MYSQL, changeEventStatus);

        verify(mySqlStreamState).setCurrentPosition(binlogCoordinate);
        verify(sourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        verify(checkpointCounter).increment();
    }

    @Test
    void test_checkpoint_postgres() {
        final LogSequenceNumber logSequenceNumber = mock(LogSequenceNumber.class);
        final StreamProgressState streamProgressState = mock(StreamProgressState.class);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamProgressState.getPostgresStreamState()).thenReturn(postgresStreamState);
        when(changeEventStatus.getLogSequenceNumber()).thenReturn(logSequenceNumber);

        streamCheckpointer.checkpoint(EngineType.POSTGRES, changeEventStatus);

        verify(postgresStreamState).setCurrentLsn(logSequenceNumber.asString());
        verify(sourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
        verify(checkpointCounter).increment();
    }

    @Test
    void test_extendLease() {
        streamCheckpointer.extendLease();

        verify(sourceCoordinator).saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    @Test
    void test_giveUpPartition() {
        streamCheckpointer.giveUpPartition();

        verify(sourceCoordinator).giveUpPartition(streamPartition);
    }

    private StreamCheckpointer createObjectUnderTest() {
        return new StreamCheckpointer(sourceCoordinator, streamPartition, pluginMetrics);
    }
}