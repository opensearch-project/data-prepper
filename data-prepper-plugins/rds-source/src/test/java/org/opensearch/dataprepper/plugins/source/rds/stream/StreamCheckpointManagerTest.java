/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
class StreamCheckpointManagerTest {

    static final Duration ACK_TIMEOUT = Duration.ofMinutes(5);

    @Mock
    private StreamCheckpointer streamCheckpointer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private Runnable stopStreamRunnable;

    private boolean isAcknowledgmentEnabled = false;

    @BeforeEach
    void setUp() {

    }

    @Test
    void test_start() {
        final ExecutorService executorService = mock(ExecutorService.class);
        try (MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(Executors::newSingleThreadExecutor).thenReturn(executorService);

            final StreamCheckpointManager streamCheckpointManager = createObjectUnderTest();
            streamCheckpointManager.start();
        }
        verify(executorService).submit(any(Runnable.class));
    }

    @Test
    void test_shutdown() {
        final ExecutorService executorService = mock(ExecutorService.class);
        try (MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(Executors::newSingleThreadExecutor).thenReturn(executorService);

            final StreamCheckpointManager streamCheckpointManager = createObjectUnderTest();
            streamCheckpointManager.start();
            streamCheckpointManager.stop();
        }

        verify(executorService).shutdownNow();
    }

    @Test
    void test_saveChangeEventsStatus() {
        final BinlogCoordinate binlogCoordinate = mock(BinlogCoordinate.class);
        final StreamCheckpointManager streamCheckpointManager = createObjectUnderTest();
        streamCheckpointManager.saveChangeEventsStatus(binlogCoordinate);

        assertThat(streamCheckpointManager.getChangeEventStatuses().size(), is(1));
        assertThat(streamCheckpointManager.getChangeEventStatuses().peek().getBinlogCoordinate(), is(binlogCoordinate));
    }

    @Test
    void test_createAcknowledgmentSet() {
        final BinlogCoordinate binlogCoordinate = mock(BinlogCoordinate.class);
        final StreamCheckpointManager streamCheckpointManager = createObjectUnderTest();
        streamCheckpointManager.createAcknowledgmentSet(binlogCoordinate);

        assertThat(streamCheckpointManager.getChangeEventStatuses().size(), is(1));
        RowChangeEventStatus changeEventStatus = streamCheckpointManager.getChangeEventStatuses().peek();
        assertThat(changeEventStatus.getBinlogCoordinate(), is(binlogCoordinate));
        verify(acknowledgementSetManager).create(any(Consumer.class), eq(ACK_TIMEOUT));
    }

    private StreamCheckpointManager createObjectUnderTest() {
        return new StreamCheckpointManager(
                streamCheckpointer, isAcknowledgmentEnabled, acknowledgementSetManager, stopStreamRunnable, ACK_TIMEOUT);
    }
}