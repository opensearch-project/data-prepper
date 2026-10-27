package org.opensearch.dataprepper.plugins.mongo.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.DataQueryProgressState;
import org.opensearch.dataprepper.plugins.mongo.utils.DocumentDBSourceAggregateMetrics;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_PREFIX;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportWorker.ACTIVE_EXPORT_PARTITION_CONSUMERS_GAUGE;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.LAST_UPDATE_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.LOADED_PARTITIONS;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.LOADED_RECORDS;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.TOTAL_PARTITIONS;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.TOTAL_PARTITIONS_COMPLETE;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.STREAM_PREFIX;

@ExtendWith(MockitoExtension.class)
public class ExportWorkerTest {
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
    private MongoDBSourceConfig sourceConfig;

    @Mock
    private RecordBufferWriter recordBufferWriter;

    @Mock
    private DocumentDBSourceAggregateMetrics documentDBSourceAggregateMetrics;

    @Mock
    private Counter partitionCounter;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    private ExportWorker exportWorker;

    @BeforeEach
    public void setup() throws Exception {
        exportWorker = new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, sourceConfig, S3_PATH_PREFIX, documentDBSourceAggregateMetrics);
    }

    /**
     * An interrupted ExportWorker calls ExportPartitionWorker.stopAll(), which sets a JVM-wide flag. Reset it so
     * a worker stopped here does not end the loading loop of ExportPartitionWorker tests run later in this JVM.
     */
    @AfterEach
    void resetExportPartitionWorkerStopFlag() throws Exception {
        final Field shouldStop = ExportPartitionWorker.class.getDeclaredField("shouldStop");
        shouldStop.setAccessible(true);
        shouldStop.setBoolean(null, false);
    }

    @Test
    void testEmptyDataQueryPartition() throws Exception {
        when(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> exportWorker.run());
        Thread.sleep(100);
        stop(executorService);

        verifyNoInteractions(recordBufferWriter);
    }

    @Test
    void test_export_withNullS3PathPrefix() {
        assertThrows(IllegalArgumentException.class, () -> new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, sourceConfig, null, documentDBSourceAggregateMetrics));
    }

    /**
     * A data query partition whose lease expired while its sink acknowledgment was pending is re-acquired and
     * loaded again by another owner. When the original owner's late acknowledgment arrives, the partition has
     * already been completed by the new owner, so it must not be counted a second time. Double counting pushes
     * loadedPartitions past totalPartitions, and the exact equality check never fires again.
     */
    @Test
    void test_lateAcknowledgment_forAlreadyCompletedPartition_isNotCountedTwice() throws Exception {
        final AckHarness harness = startWorkerAndCaptureAcknowledgment(1L, 0L, false);
        // The new owner already completed this partition, so the late owner's write is stale.
        willAnswer(invocation -> null)
                .willThrow(new PartitionUpdateException("ConditionalCheckFailed: partition was modified by another owner", null))
                .given(sourceCoordinator).completePartition(any());

        // New owner's acknowledgment.
        harness.ackCallback.accept(true);
        // Original owner's late acknowledgment for the same partition.
        try {
            harness.ackCallback.accept(true);
        } catch (final PartitionUpdateException ignored) {
            // The current code lets the stale completion failure escape from the callback.
        } finally {
            stop(harness.executorService);
        }

        assertThat(harness.storedState.get().get(LOADED_PARTITIONS), equalTo(1L));
    }

    /**
     * Once the total partition count is final, the acknowledgment that finishes the export must create the
     * STREAM global state, even if loadedPartitions has already been pushed past totalPartitions by an
     * earlier duplicate count. With an exact equality check the trigger is skipped and the stream worker
     * waits for the initial load forever.
     */
    @Test
    void test_finalAcknowledgment_whenLoadedAlreadyExceedsTotal_stillCreatesStreamState() throws Exception {
        // 3 partitions in total, 2 really loaded plus 1 duplicate count, flag already final.
        final AckHarness harness = startWorkerAndCaptureAcknowledgment(3L, 3L, true);

        try {
            harness.ackCallback.accept(true);
        } finally {
            stop(harness.executorService);
        }

        final ArgumentCaptor<EnhancedSourcePartition> created = ArgumentCaptor.forClass(EnhancedSourcePartition.class);
        verify(sourceCoordinator, atLeastOnce()).createPartition(created.capture());
        final List<String> createdKeys = created.getAllValues().stream()
                .map(EnhancedSourcePartition::getPartitionKey)
                .collect(Collectors.toList());
        assertThat(createdKeys, hasItem(STREAM_PREFIX + harness.collection));
    }

    private static void stop(final ExecutorService executorService) throws InterruptedException {
        executorService.shutdownNow();
        // Wait for the worker to finish its shutdown, including stopAll(), before the flag is reset.
        assertThat(executorService.awaitTermination(10, TimeUnit.SECONDS), equalTo(true));
    }

    private static final class AckHarness {
        private final String collection;
        private final AtomicReference<Map<String, Object>> storedState;
        private final Consumer<Boolean> ackCallback;
        private final ExecutorService executorService;

        private AckHarness(final String collection,
                           final AtomicReference<Map<String, Object>> storedState,
                           final Consumer<Boolean> ackCallback,
                           final ExecutorService executorService) {
            this.collection = collection;
            this.storedState = storedState;
            this.ackCallback = ackCallback;
            this.executorService = executorService;
        }
    }

    /**
     * Runs the export worker with acknowledgments enabled against an in-memory EXPORT global state, lets it
     * acquire one data query partition, and returns the acknowledgment callback it registered for that partition.
     */
    private AckHarness startWorkerAndCaptureAcknowledgment(final long totalPartitions,
                                                           final long loadedPartitions,
                                                           final boolean totalPartitionsComplete) {
        final String collection = "db." + UUID.randomUUID();
        final String exportStateKey = EXPORT_PREFIX + collection;

        // Lenient: the partition loader started by the worker also registers its own metrics.
        lenient().when(pluginMetrics.counter(anyString())).thenReturn(partitionCounter);
        given(pluginMetrics.gauge(eq(ACTIVE_EXPORT_PARTITION_CONSUMERS_GAUGE), any(AtomicInteger.class)))
                .willAnswer(invocation -> invocation.getArgument(1));
        given(sourceConfig.isAcknowledgmentsEnabled()).willReturn(true);
        given(sourceConfig.getPartitionAcknowledgmentTimeout()).willReturn(Duration.ofMinutes(30));

        final DataQueryProgressState progressState = new DataQueryProgressState();
        progressState.setLoadedRecords(10L);
        final DataQueryPartition dataQueryPartition = new DataQueryPartition(collection + "|" + UUID.randomUUID(), progressState);
        given(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE))
                .willReturn(Optional.of(dataQueryPartition), Optional.empty());

        final AtomicReference<Map<String, Object>> storedState = new AtomicReference<>(new HashMap<>(Map.of(
                TOTAL_PARTITIONS, totalPartitions,
                LOADED_PARTITIONS, loadedPartitions,
                LOADED_RECORDS, 0L,
                LAST_UPDATE_TIMESTAMP, Instant.now().toEpochMilli(),
                TOTAL_PARTITIONS_COMPLETE, totalPartitionsComplete)));
        // Every read returns a fresh copy of the stored EXPORT state; other global states do not exist yet.
        given(sourceCoordinator.getPartition(anyString())).willAnswer(invocation ->
                exportStateKey.equals(invocation.getArgument(0))
                        ? Optional.of(new GlobalState(exportStateKey, new HashMap<>(storedState.get())))
                        : Optional.empty());
        willAnswer(invocation -> {
            final EnhancedSourcePartition<?> partition = invocation.getArgument(0);
            if (exportStateKey.equals(partition.getPartitionKey())) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> saved = (Map<String, Object>) partition.getProgressState().get();
                storedState.set(new HashMap<>(saved));
            }
            return null;
        }).given(sourceCoordinator).saveProgressStateForPartition(any(), any());

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);
        given(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class))).willReturn(acknowledgementSet);

        final ExportWorker worker = new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager,
                sourceConfig, S3_PATH_PREFIX, documentDBSourceAggregateMetrics);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(worker::run);
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> verify(acknowledgementSetManager).create(any(), any(Duration.class)));

        return new AckHarness(collection, storedState, callbackCaptor.getValue(), executorService);
    }
}
