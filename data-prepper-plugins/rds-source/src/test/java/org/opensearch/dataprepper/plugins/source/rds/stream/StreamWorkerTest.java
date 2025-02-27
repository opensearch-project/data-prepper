/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.MySqlStreamState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamWorkerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private BinlogClientWrapper binlogClientWrapper;

    @Mock
    private BinaryLogClient binaryLogClient;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private StreamPartition streamPartition;

    private StreamWorker streamWorker;

    @BeforeEach
    void setUp() {
        streamWorker = createObjectUnderTest();
    }

    @Test
    void test_processStream_with_given_binlog_coordinates() throws IOException {
        final StreamProgressState streamProgressState = mock(StreamProgressState.class);
        final MySqlStreamState mySqlStreamState = mock(MySqlStreamState.class);
        final String binlogFilename = UUID.randomUUID().toString();
        final long binlogPosition = 100L;
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamProgressState.getMySqlStreamState()).thenReturn(mySqlStreamState);
        when(mySqlStreamState.getCurrentPosition()).thenReturn(new BinlogCoordinate(binlogFilename, binlogPosition));
        when(streamProgressState.shouldWaitForExport()).thenReturn(false);
        when(binlogClientWrapper.getBinlogClient()).thenReturn(binaryLogClient);

        streamWorker.processStream(streamPartition);

        verify(binaryLogClient).setBinlogFilename(binlogFilename);
        verify(binaryLogClient).setBinlogPosition(binlogPosition);
        verify(binlogClientWrapper).connect();
    }

    @Test
    void test_processStream_without_current_binlog_coordinates() throws IOException {
        final StreamProgressState streamProgressState = mock(StreamProgressState.class);
        final MySqlStreamState mySqlStreamState = mock(MySqlStreamState.class);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        final String binlogFilename = "binlog-001";
        final long binlogPosition = 100L;
        when(streamProgressState.getMySqlStreamState()).thenReturn(mySqlStreamState);
        when(mySqlStreamState.getCurrentPosition()).thenReturn(null);
        when(streamProgressState.shouldWaitForExport()).thenReturn(false);

        streamWorker.processStream(streamPartition);

        verify(binaryLogClient, never()).setBinlogFilename(binlogFilename);
        verify(binaryLogClient, never()).setBinlogPosition(binlogPosition);
        verify(binlogClientWrapper).connect();
    }

    @Test
    void test_shutdown() throws IOException {
        streamWorker.shutdown();
        verify(binlogClientWrapper).disconnect();
    }

    private StreamWorker createObjectUnderTest() {
        return new StreamWorker(sourceCoordinator, binlogClientWrapper, pluginMetrics);
    }
}