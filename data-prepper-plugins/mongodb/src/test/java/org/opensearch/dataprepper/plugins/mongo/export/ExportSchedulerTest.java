package org.opensearch.dataprepper.plugins.mongo.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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

@ExtendWith(MockitoExtension.class)
public class ExportSchedulerTest {
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

    private ExportScheduler exportScheduler;
    private ExportPartition exportPartition;

    @BeforeEach
    void setup() throws Exception {
        given(pluginMetrics.counter(EXPORT_JOB_SUCCESS_COUNT)).willReturn(exportJobSuccessCounter);
        given(pluginMetrics.counter(EXPORT_JOB_FAILURE_COUNT)).willReturn(exportJobFailureCounter);
        given(pluginMetrics.counter(EXPORT_PARTITION_QUERY_TOTAL_COUNT)).willReturn(exportPartitionTotalCounter);
        given(pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT)).willReturn(exportRecordsTotalCounter);

    }

    @Test
    void test_no_export_run() throws InterruptedException {
        exportScheduler = new ExportScheduler(coordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> exportScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();

        verifyNoInteractions(mongoDBExportPartitionSupplier);
        verify(coordinator, never()).createPartition(any());
    }

    @Test
    void test_export_run() throws InterruptedException {
        exportScheduler = new ExportScheduler(coordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        final String collection = UUID.randomUUID().toString();
        final int partitionSize = new Random().nextInt();
        final Instant exportTime = Instant.now();
        final String partitionKey = collection + "|" + UUID.randomUUID();
        final String globalPartitionKey = collection + "|" + partitionSize + "|" + exportTime.toEpochMilli();

        exportPartition = new ExportPartition(collection, partitionSize, exportTime, null);
        given(partitionIdentifier.getPartitionKey()).willReturn(partitionKey);
        given(mongoDBExportPartitionSupplier.apply(exportPartition)).willReturn(List.of(partitionIdentifier));
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.of(exportPartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> exportScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();


        // Acquire the init partition
        verify(coordinator).acquireAvailablePartition(eq(ExportPartition.PARTITION_TYPE));

        final ArgumentCaptor<EnhancedSourcePartition> argumentCaptor = ArgumentCaptor.forClass(EnhancedSourcePartition.class);
        // Should create 1 export partition + 1 stream partitions + 1 global table state
        verify(coordinator, times(2)).createPartition(argumentCaptor.capture());
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

        var globalStates = partitions.stream()
                .filter(partition -> partition instanceof GlobalState)
                .map(partition -> (GlobalState)partition).collect(Collectors.toList());
        assertThat(globalStates.size(), equalTo(1));
        globalStates.forEach(globalState -> {
            assertThat(globalState.getPartitionKey(), equalTo(EXPORT_PREFIX + globalPartitionKey));
            assertThat(globalState.getProgressState().get().toString(), is(Map.of(
                    "totalPartitions", 1,
                    "loadedPartitions", 0,
                    "loadedRecords", 0
            ).toString()));
            assertThat(globalState.getPartitionType(), equalTo(null));
        });
        verify(exportPartitionTotalCounter).increment(1);
    }

    @Test
    void test_export_run_multiple_partitions() throws InterruptedException {
        exportScheduler = new ExportScheduler(coordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        final String collection = UUID.randomUUID().toString();
        final int partitionSize = new Random().nextInt();
        final Instant exportTime = Instant.now();
        final String partitionKey = collection + "|" + UUID.randomUUID();
        final String globalPartitionKey = collection + "|" + partitionSize + "|" + exportTime.toEpochMilli();

        exportPartition = new ExportPartition(collection, partitionSize, exportTime, null);
        given(partitionIdentifier.getPartitionKey()).willReturn(partitionKey);
        given(mongoDBExportPartitionSupplier.apply(exportPartition)).willReturn(List.of(partitionIdentifier, partitionIdentifier, partitionIdentifier));
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.of(exportPartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> exportScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();


        // Acquire the init partition
        verify(coordinator).acquireAvailablePartition(eq(ExportPartition.PARTITION_TYPE));

        final ArgumentCaptor<EnhancedSourcePartition> argumentCaptor = ArgumentCaptor.forClass(EnhancedSourcePartition.class);
        // Should create 1 export partition + 1 stream partitions + 1 global table state
        verify(coordinator, times(4)).createPartition(argumentCaptor.capture());
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

        var globalStates = partitions.stream()
                .filter(partition -> partition instanceof GlobalState)
                .map(partition -> (GlobalState)partition).collect(Collectors.toList());
        assertThat(globalStates.size(), equalTo(1));
        globalStates.forEach(globalState -> {
            assertThat(globalState.getPartitionKey(), equalTo(EXPORT_PREFIX + globalPartitionKey));
            assertThat(globalState.getProgressState().get().toString(), is(Map.of(
                    "totalPartitions", 3,
                    "loadedPartitions", 0,
                    "loadedRecords", 0
            ).toString()));
            assertThat(globalState.getPartitionType(), equalTo(null));
        });
        verify(exportPartitionTotalCounter).increment(3);
    }
}
