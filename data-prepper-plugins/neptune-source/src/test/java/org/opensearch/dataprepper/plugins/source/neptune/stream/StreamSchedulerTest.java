package org.opensearch.dataprepper.plugins.source.neptune.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.neptune.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamScheduler.DEFAULT_BUFFER_WRITE_INTERVAL_MILLS;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamScheduler.DEFAULT_CHECKPOINT_INTERVAL_MILLS;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamScheduler.DEFAULT_RECORD_FLUSH_BATCH_SIZE;


@ExtendWith(MockitoExtension.class)
public class StreamSchedulerTest {
    private final String S3_PATH_PREFIX = UUID.randomUUID().toString();
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private NeptuneSourceConfig sourceConfig;

    @Mock
    private StreamWorker streamWorker;


    private StreamScheduler streamScheduler;

    @BeforeEach
    void setup() {
        streamScheduler = new StreamScheduler(sourceCoordinator, buffer, acknowledgementSetManager, sourceConfig, S3_PATH_PREFIX, pluginMetrics);
    }


    @Test
    void test_no_stream_run() throws InterruptedException {
        given(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> streamScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();

        verifyNoInteractions(streamWorker);
    }

    @Test
    void test_stream_run() {
        final StreamProgressState streamProgressState = new StreamProgressState();
        final StreamPartition streamPartition = new StreamPartition(streamProgressState);
        given(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willReturn(Optional.of(streamPartition));
        final int streamBatchSize = 100;

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> {
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class)) {
                streamWorkerMockedStatic.when(() -> StreamWorker.create(any(RecordBufferWriter.class), any(StreamRecordConverter.class), eq(sourceConfig),
                                any(StreamAcknowledgementManager.class), any(DataStreamPartitionCheckpoint.class), eq(pluginMetrics), eq(DEFAULT_RECORD_FLUSH_BATCH_SIZE),
                                eq(DEFAULT_CHECKPOINT_INTERVAL_MILLS), eq(DEFAULT_BUFFER_WRITE_INTERVAL_MILLS), eq(streamBatchSize)))
                        .thenReturn(streamWorker);
                streamScheduler.run();
            }
        });

        await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> verify(streamWorker).processStream(eq(streamPartition)));

        future.cancel(true);
        executorService.shutdownNow();

    }

    @Test
    void test_stream_runThrowsException() {
        final StreamProgressState streamProgressState = new StreamProgressState();
        final StreamPartition streamPartition = new StreamPartition(streamProgressState);
        given(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willReturn(Optional.of(streamPartition));
        final int streamBatchSize = 100;

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> {
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class)) {
                streamWorkerMockedStatic.when(() -> StreamWorker.create(any(RecordBufferWriter.class), any(StreamRecordConverter.class), eq(sourceConfig),
                                any(StreamAcknowledgementManager.class), any(DataStreamPartitionCheckpoint.class), eq(pluginMetrics), eq(DEFAULT_RECORD_FLUSH_BATCH_SIZE),
                                eq(DEFAULT_CHECKPOINT_INTERVAL_MILLS), eq(DEFAULT_BUFFER_WRITE_INTERVAL_MILLS), eq(streamBatchSize)))
                        .thenThrow(RuntimeException.class);
                streamScheduler.run();
            }
        });

        await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> verify(sourceCoordinator).giveUpPartition(streamPartition));

        future.cancel(true);
        executorService.shutdownNow();

    }

    @Test
    void test_stream_sourceCoordinatorThrowsException() {
        final StreamProgressState streamProgressState = new StreamProgressState();

        final StreamPartition streamPartition = new StreamPartition(streamProgressState);
        given(sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> streamScheduler.run());

        await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> verify(sourceCoordinator, never()).giveUpPartition(streamPartition));

        future.cancel(true);
        executorService.shutdownNow();

    }

    @Test
    void test_stream_withNullS3PathPrefix() {
        assertThrows(IllegalArgumentException.class, () -> new StreamScheduler(sourceCoordinator, buffer, acknowledgementSetManager, sourceConfig, null, pluginMetrics));
    }
}
