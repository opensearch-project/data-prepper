package org.opensearch.dataprepper.plugins.mongo.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.mongo.model.PartitionIdentifierBatch;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_JOB_FAILURE_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_JOB_SUCCESS_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_PARTITION_QUERY_TOTAL_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_PREFIX;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_RECORDS_TOTAL_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.DEFAULT_GET_PARTITION_BACKOFF_MILLIS;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.LAST_UPDATE_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.LOADED_PARTITIONS;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.LOADED_RECORDS;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.TOTAL_PARTITIONS;
import static org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus.TOTAL_PARTITIONS_COMPLETE;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.STREAM_PREFIX;

@ExtendWith(MockitoExtension.class)
public class ExportSchedulerTest {
    private static final Random RANDOM = new Random();
    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private MongoDBExportPartitionSupplier mongoDBExportPartitionSupplier;

    @Mock
    private PartitionIdentifier partitionIdentifier;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter exportJobSuccessCounter;

    @Mock
    private Counter exportJobFailureCounter;

    @Mock
    private Counter exportPartitionTotalCounter;

    @Mock
    private Counter exportRecordsTotalCounter;

    @Mock
    private PartitionIdentifierBatch partitionIdentifierBatch;

    @Mock
    private GlobalState globalState;

    @Captor
    private ArgumentCaptor<Map<String, Object>> progressStateCaptor;

    private ExportScheduler exportScheduler;
    private ExportPartition exportPartition;

    @BeforeEach
    void setup() {
        given(pluginMetrics.counter(EXPORT_JOB_SUCCESS_COUNT)).willReturn(exportJobSuccessCounter);
        given(pluginMetrics.counter(EXPORT_JOB_FAILURE_COUNT)).willReturn(exportJobFailureCounter);
        given(pluginMetrics.counter(EXPORT_PARTITION_QUERY_TOTAL_COUNT)).willReturn(exportPartitionTotalCounter);
        given(pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT)).willReturn(exportRecordsTotalCounter);

    }

    @Test
    void test_no_export_run() {
        exportScheduler = new ExportScheduler(coordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> exportScheduler.run());
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(coordinator).acquireAvailablePartition(eq(ExportPartition.PARTITION_TYPE)));

        future.cancel(true);
        verifyNoInteractions(mongoDBExportPartitionSupplier);
        verify(coordinator, never()).createPartition(any());
        executorService.shutdownNow();
    }

    @Test
    void test_export_run() {
        exportScheduler = new ExportScheduler(coordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        final String collection = UUID.randomUUID().toString();
        final int partitionSize = new Random().nextInt();
        final Instant exportTime = Instant.now();
        final String partitionKey = collection + "|" + UUID.randomUUID();

        final ExportProgressState exportProgressState = new ExportProgressState();
        exportProgressState.setCollectionName(collection);
        exportProgressState.setDatabaseName(collection);
        exportProgressState.setExportTime(exportTime.toString());

        exportPartition = new ExportPartition(collection, partitionSize, exportTime, exportProgressState);
        given(partitionIdentifier.getPartitionKey()).willReturn(partitionKey);
        given(mongoDBExportPartitionSupplier.apply(exportPartition)).willReturn(partitionIdentifierBatch);
        given(partitionIdentifierBatch.getPartitionIdentifiers()).willReturn(List.of(partitionIdentifier));
        given(partitionIdentifierBatch.isLastBatch()).willReturn(true);
        given(partitionIdentifierBatch.getEndDocId()).willReturn(UUID.randomUUID().toString());
        given(coordinator.getPartition(eq(EXPORT_PREFIX + collection))).willReturn(
                Optional.empty(), Optional.of(globalState));
        final long totalPartitions = Integer.valueOf(RANDOM.nextInt(10)).longValue() + 1;
        final Instant lastUpdateTimestamp = Instant.now().minus(1, ChronoUnit.MINUTES);
        final Map<String, Object> progressState1 = Map.of(
                TOTAL_PARTITIONS, totalPartitions,
                LOADED_PARTITIONS, 0L,
                LOADED_RECORDS, 0L,
                LAST_UPDATE_TIMESTAMP, lastUpdateTimestamp.toEpochMilli(),
                TOTAL_PARTITIONS_COMPLETE, false
        );
        final Map<String, Object> progressState2 = Map.of(
                TOTAL_PARTITIONS, totalPartitions + 1,
                LOADED_PARTITIONS, 0L,
                LOADED_RECORDS, 0L,
                LAST_UPDATE_TIMESTAMP, lastUpdateTimestamp.toEpochMilli(),
                TOTAL_PARTITIONS_COMPLETE, false
        );
        given(globalState.getProgressState()).willReturn(Optional.of(progressState1)).willReturn(Optional.of(progressState2));
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.of(exportPartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> exportScheduler.run());
        await()
            .atMost(Duration.ofMillis(DEFAULT_GET_PARTITION_BACKOFF_MILLIS).plus(Duration.ofSeconds(2)))
            .untilAsserted(() -> verify(coordinator, times(1)).createPartition(any()));

        future.cancel(true);

        verify(coordinator, times(3)).getPartition(eq(EXPORT_PREFIX + collection));

        // Acquire the init partition
        verify(coordinator).acquireAvailablePartition(eq(ExportPartition.PARTITION_TYPE));

        final ArgumentCaptor<EnhancedSourcePartition> argumentCaptor = ArgumentCaptor.forClass(EnhancedSourcePartition.class);
        // Should create 1 data query partition
        verify(coordinator, times(1)).createPartition(argumentCaptor.capture());
        final List<EnhancedSourcePartition> partitions = argumentCaptor.getAllValues();
        var dataQueryPartitions = partitions.stream()
            .filter(partition -> partition instanceof DataQueryPartition)
            .map(partition -> (DataQueryPartition)partition).collect(Collectors.toList());
        assertThat(dataQueryPartitions.size(), equalTo(1));
        dataQueryPartitions.forEach(dataQueryPartition -> {
            assertThat(dataQueryPartition.getCollection(), equalTo(collection));
            assertThat(dataQueryPartition.getPartitionKey(), equalTo(partitionKey));
            assertThat(dataQueryPartition.getQuery(), equalTo(partitionKey));
            assertThat(partitions.get(0).getPartitionType(), equalTo(DataQueryPartition.PARTITION_TYPE));
        });

        verify(globalState, times(2)).setProgressState(progressStateCaptor.capture());
        final Map<String, Object> updatedProgressState = progressStateCaptor.getValue();
        assertThat(updatedProgressState.get(TOTAL_PARTITIONS), equalTo(totalPartitions + 1));
        assertThat(updatedProgressState.get(LOADED_PARTITIONS), is(0L));
        assertThat(updatedProgressState.get(LOADED_RECORDS), is(0L));
        assertThat((Long) updatedProgressState.get(LAST_UPDATE_TIMESTAMP),
                is(greaterThanOrEqualTo(lastUpdateTimestamp.toEpochMilli())));
        verify(coordinator).saveProgressStateForPartition(eq(exportPartition), eq(null));
        verify(coordinator, times(2)).saveProgressStateForPartition(eq(globalState), any());
        verify(exportPartitionTotalCounter).increment(1);
        executorService.shutdownNow();
    }

    @Test
    void test_export_run_multiple_partitions() {
        exportScheduler = new ExportScheduler(coordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        final String collection = UUID.randomUUID().toString();
        final int partitionSize = new Random().nextInt();
        final Instant exportTime = Instant.now();
        final String partitionKey = collection + "|" + UUID.randomUUID();

        final ExportProgressState exportProgressState = new ExportProgressState();
        exportProgressState.setCollectionName(collection);
        exportProgressState.setDatabaseName(collection);
        exportProgressState.setExportTime(exportTime.toString());
        
        exportPartition = new ExportPartition(collection, partitionSize, exportTime, exportProgressState);
        given(partitionIdentifier.getPartitionKey()).willReturn(partitionKey);
        given(mongoDBExportPartitionSupplier.apply(exportPartition)).willReturn(partitionIdentifierBatch);
        given(partitionIdentifierBatch.getPartitionIdentifiers()).willReturn(List.of(partitionIdentifier, partitionIdentifier, partitionIdentifier));
        given(coordinator.getPartition(eq(EXPORT_PREFIX + collection))).willReturn(Optional.of(globalState));
        final long totalPartitions = Integer.valueOf(RANDOM.nextInt(10)).longValue();
        final Instant lastUpdateTimestamp = Instant.now().minus(1, ChronoUnit.MINUTES);
        final Map<String, Object> progressState = Map.of(
                TOTAL_PARTITIONS, totalPartitions,
                LOADED_PARTITIONS, 0L,
                LOADED_RECORDS, 0L,
                LAST_UPDATE_TIMESTAMP, lastUpdateTimestamp.toEpochMilli(),
                TOTAL_PARTITIONS_COMPLETE, false
        );
        given(globalState.getProgressState()).willReturn(Optional.of(progressState));
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.of(exportPartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> exportScheduler.run());
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> verify(coordinator, times(3)).createPartition(any()));

        future.cancel(true);

        // Acquire the init partition
        verify(coordinator).acquireAvailablePartition(eq(ExportPartition.PARTITION_TYPE));

        final ArgumentCaptor<EnhancedSourcePartition> argumentCaptor = ArgumentCaptor.forClass(EnhancedSourcePartition.class);
        // Should create 3 data query partitions
        verify(coordinator, times(3)).createPartition(argumentCaptor.capture());
        final List<EnhancedSourcePartition> partitions = argumentCaptor.getAllValues();
        var dataQueryPartitions = partitions.stream()
                .filter(partition -> partition instanceof DataQueryPartition)
                .map(partition -> (DataQueryPartition)partition).collect(Collectors.toList());
        assertThat(dataQueryPartitions.size(), equalTo(3));
        dataQueryPartitions.forEach(dataQueryPartition -> {
            assertThat(dataQueryPartition.getCollection(), equalTo(collection));
            assertThat(dataQueryPartition.getPartitionKey(), equalTo(partitionKey));
            assertThat(dataQueryPartition.getQuery(), equalTo(partitionKey));
            assertThat(partitions.get(0).getPartitionType(), equalTo(DataQueryPartition.PARTITION_TYPE));
        });

        verify(globalState).setProgressState(progressStateCaptor.capture());
        final Map<String, Object> updatedProgressState = progressStateCaptor.getValue();
        assertThat(updatedProgressState.get(TOTAL_PARTITIONS), equalTo(totalPartitions + 3));
        assertThat(updatedProgressState.get(LOADED_PARTITIONS), is(0L));
        assertThat(updatedProgressState.get(LOADED_RECORDS), is(0L));
        assertThat((Long) updatedProgressState.get(LAST_UPDATE_TIMESTAMP),
                is(greaterThanOrEqualTo(lastUpdateTimestamp.toEpochMilli())));
        verify(coordinator).saveProgressStateForPartition(eq(globalState), any());
        verify(exportPartitionTotalCounter).increment(3);
        executorService.shutdownNow();
    }

    @Test
    void test_exportRun_emptyPartitionIdentifier() {
        exportScheduler = new ExportScheduler(coordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        final String collection = UUID.randomUUID().toString();
        final int partitionSize = new Random().nextInt();
        final Instant exportTime = Instant.now();
        final ExportProgressState exportProgressState = new ExportProgressState();
        exportProgressState.setCollectionName(collection);
        exportProgressState.setDatabaseName(collection);
        exportProgressState.setExportTime(exportTime.toString());
        exportPartition = new ExportPartition(collection, partitionSize, exportTime, exportProgressState);
        given(mongoDBExportPartitionSupplier.apply(exportPartition)).willReturn(partitionIdentifierBatch);
        given(partitionIdentifierBatch.getPartitionIdentifiers()).willReturn(Collections.emptyList());
        given(partitionIdentifierBatch.isLastBatch()).willReturn(true);
        given(coordinator.getPartition(eq(EXPORT_PREFIX + collection))).willReturn(
                Optional.empty(), Optional.of(globalState));
        final long totalPartitions = Integer.valueOf(RANDOM.nextInt(10)).longValue();
        final Instant lastUpdateTimestamp = Instant.now().minus(1, ChronoUnit.MINUTES);
        final Map<String, Object> progressState = Map.of(
                TOTAL_PARTITIONS, totalPartitions,
                LOADED_PARTITIONS, 0L,
                LOADED_RECORDS, 0L,
                LAST_UPDATE_TIMESTAMP, lastUpdateTimestamp.toEpochMilli(),
                TOTAL_PARTITIONS_COMPLETE, false
        );
        given(globalState.getProgressState()).willReturn(Optional.of(progressState));
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.of(exportPartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> exportScheduler.run());
        await()
                .atMost(Duration.ofMillis(DEFAULT_GET_PARTITION_BACKOFF_MILLIS).plus(Duration.ofSeconds(2)))
                .untilAsserted(() -> verify(coordinator).completePartition(exportPartition));

        future.cancel(true);

        verify(coordinator, times(3)).getPartition(eq(EXPORT_PREFIX + collection));

        // Acquire the init partition
        verify(coordinator).acquireAvailablePartition(eq(ExportPartition.PARTITION_TYPE));
        final ArgumentCaptor<GlobalState> argumentCaptor = ArgumentCaptor.forClass(GlobalState.class);
        verify(coordinator).createPartition(argumentCaptor.capture());
        final GlobalState streamGlobalState = argumentCaptor.getValue();
        assertThat(streamGlobalState.getPartitionKey(), is(STREAM_PREFIX + exportPartition.getCollection()));
        verify(coordinator).saveProgressStateForPartition(eq(globalState), eq(null));
        executorService.shutdownNow();
    }
}
