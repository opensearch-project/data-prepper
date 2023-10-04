/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.SourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.LoadStatus;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileScheduler.EXPORT_FILE_SUCCESS_COUNT;

@ExtendWith(MockitoExtension.class)
class DataFileSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private GlobalState tableInfoGlobalState;

    @Mock
    private GlobalState exportInfoGlobalState;

    @Mock
    private Counter exportFileSuccess;

    @Mock
    private DataFileLoaderFactory loaderFactory;

    private DataFileScheduler scheduler;

    private DataFilePartition dataFilePartition;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;
    private final String manifestKey = UUID.randomUUID().toString();
    private final String bucketName = UUID.randomUUID().toString();
    private final String prefix = UUID.randomUUID().toString();

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";


    @BeforeEach
    void setup() {


        DataFileProgressState state = new DataFileProgressState();
        state.setLoaded(0);
        state.setTotal(100);
//        lenient().when(dataFilePartition.getProgressState()).thenReturn(Optional.of(state));

        dataFilePartition = new DataFilePartition(exportArn, bucketName, manifestKey, Optional.of(state));

        // Mock Global Table Info
        lenient().when(coordinator.getPartition(tableArn)).thenReturn(Optional.of(tableInfoGlobalState));
        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName("PK")
                .sortKeyAttributeName("SK")
                .streamArn(streamArn)
                .build();
//        Map<String, Object> tableState = metadata;
        lenient().when(tableInfoGlobalState.getProgressState()).thenReturn(Optional.of(metadata.toMap()));


        // Mock Global Export Info
        LoadStatus loadStatus = new LoadStatus(1, 0, 100, 0);
        lenient().when(coordinator.getPartition(exportArn)).thenReturn(Optional.of(exportInfoGlobalState));
        lenient().when(exportInfoGlobalState.getProgressState()).thenReturn(Optional.of(loadStatus.toMap()));

        given(pluginMetrics.counter(EXPORT_FILE_SUCCESS_COUNT)).willReturn(exportFileSuccess);

        lenient().when(coordinator.createPartition(any(SourcePartition.class))).thenReturn(true);
        lenient().doNothing().when(coordinator).completePartition(any(SourcePartition.class));
        lenient().doNothing().when(coordinator).giveUpPartition(any(SourcePartition.class));

        lenient().when(loaderFactory.createDataFileLoader(any(DataFilePartition.class), any(TableInfo.class))).thenReturn(() -> System.out.println("Hello"));

    }

    @Test
    public void test_run_DataFileLoader_correctly() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE)).willReturn(Optional.of(dataFilePartition)).willReturn(Optional.empty());

        scheduler = new DataFileScheduler(coordinator, loaderFactory, pluginMetrics);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(scheduler);

        // Run for a while
        Thread.sleep(500);

        // Should acquire data file partition
        verify(coordinator).acquireAvailablePartition(DataFilePartition.PARTITION_TYPE);
        // Should create a loader
        verify(loaderFactory).createDataFileLoader(any(DataFilePartition.class), any(TableInfo.class));
        // Need to call getPartition for 3 times (3 global states, 2 TableInfo)
        verify(coordinator, times(3)).getPartition(anyString());
        // Should update global state with load status
        verify(coordinator).saveProgressStateForPartition(any(GlobalState.class));
        // Should create a partition to inform streaming can start.
        verify(coordinator).createPartition(any(GlobalState.class));
        // Should mask the partition as completed.
        verify(coordinator).completePartition(any(DataFilePartition.class));
        // Should update metrics.
        verify(exportFileSuccess).increment();

        executor.shutdownNow();


    }


}