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
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileScheduler.ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileScheduler.EXPORT_S3_OBJECTS_PROCESSED_COUNT;

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
    private AtomicLong activeExportS3ObjectConsumers;

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

        lenient().when(tableInfoGlobalState.getProgressState()).thenReturn(Optional.of(metadata.toMap()));


        // Mock Global Export Info
        LoadStatus loadStatus = new LoadStatus(1, 0, 100, 0);
        lenient().when(coordinator.getPartition(exportArn)).thenReturn(Optional.of(exportInfoGlobalState));
        lenient().when(exportInfoGlobalState.getProgressState()).thenReturn(Optional.of(loadStatus.toMap()));

        given(pluginMetrics.counter(EXPORT_S3_OBJECTS_PROCESSED_COUNT)).willReturn(exportFileSuccess);
        given(pluginMetrics.gauge(eq(ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE), any(AtomicLong.class))).willReturn(activeExportS3ObjectConsumers);

        lenient().when(coordinator.createPartition(any(EnhancedSourcePartition.class))).thenReturn(true);
        lenient().doNothing().when(coordinator).completePartition(any(EnhancedSourcePartition.class));
        lenient().doNothing().when(coordinator).giveUpPartition(any(EnhancedSourcePartition.class));

        lenient().when(loaderFactory.createDataFileLoader(any(DataFilePartition.class), any(TableInfo.class))).thenReturn(() -> System.out.println("Hello"));

    }

    @Test
    public void test_run_DataFileLoader_correctly() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE)).willReturn(Optional.of(dataFilePartition)).willReturn(Optional.empty());

        scheduler = new DataFileScheduler(coordinator, loaderFactory, pluginMetrics);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> scheduler.run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS), equalTo(true));

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

        executorService.shutdownNow();


    }


}