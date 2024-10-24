/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.model.LoadStatus;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler.ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE;
import static org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler.EXPORT_S3_OBJECTS_PROCESSED_COUNT;

@ExtendWith(MockitoExtension.class)
class DataFileSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private S3Client s3Client;

    @Mock
    private EventFactory eventFactory;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private DataFilePartition dataFilePartition;

    @Mock
    private Counter exportFileSuccessCounter;

    @Mock
    private Counter exportFileErrorCounter;

    @Mock
    private AtomicInteger activeExportS3ObjectConsumersGauge;

    private Random random;
    private String s3Prefix;

    @BeforeEach
    void setUp() {
        random = new Random();
        s3Prefix = UUID.randomUUID().toString();
        when(pluginMetrics.counter(EXPORT_S3_OBJECTS_PROCESSED_COUNT)).thenReturn(exportFileSuccessCounter);
        when(pluginMetrics.counter(eq(DataFileScheduler.EXPORT_S3_OBJECTS_ERROR_COUNT))).thenReturn(exportFileErrorCounter);
        when(pluginMetrics.gauge(eq(ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE), any(AtomicInteger.class), any()))
                .thenReturn(activeExportS3ObjectConsumersGauge);
    }

    @Test
    void test_given_no_datafile_partition_then_no_export() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE)).thenReturn(Optional.empty());


        final DataFileScheduler objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(objectUnderTest);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(DataFilePartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verifyNoInteractions(s3Client, buffer, exportFileSuccessCounter, activeExportS3ObjectConsumersGauge);
    }

    @Test
    void test_given_available_datafile_partition_then_load_datafile() throws InterruptedException {
        final String exportTaskId = UUID.randomUUID().toString();
        when(dataFilePartition.getExportTaskId()).thenReturn(exportTaskId);

        when(sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE)).thenReturn(Optional.of(dataFilePartition));
        final GlobalState globalStatePartition = mock(GlobalState.class);
        final int totalFiles = random.nextInt() + 1;
        final Map<String, Object> loadStatusMap = new LoadStatus(totalFiles, totalFiles - 1).toMap();
        when(globalStatePartition.getProgressState()).thenReturn(Optional.of(loadStatusMap));
        when(sourceCoordinator.getPartition(exportTaskId)).thenReturn(Optional.of(globalStatePartition));

        DataFileScheduler objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
                    // MockedStatic needs to be created on the same thread it's used
                    try (MockedStatic<DataFileLoader> dataFileLoaderMockedStatic = mockStatic(DataFileLoader.class)) {
                        DataFileLoader dataFileLoader = mock(DataFileLoader.class);
                        dataFileLoaderMockedStatic.when(() -> DataFileLoader.create(eq(dataFilePartition), any(InputCodec.class),
                                        any(Buffer.class), any(S3ObjectReader.class),
                                        any(ExportRecordConverter.class), any(PluginMetrics.class),
                                        any(EnhancedSourceCoordinator.class), any(), any(Duration.class)))
                                .thenReturn(dataFileLoader);
                        doNothing().when(dataFileLoader).run();
                        objectUnderTest.run();
                    }
        });
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).completePartition(dataFilePartition));
        executorService.shutdownNow();

        verify(exportFileSuccessCounter).increment();
        verify(exportFileErrorCounter, never()).increment();
        verify(sourceCoordinator).completePartition(dataFilePartition);
    }

    @Test
    void test_data_file_loader_throws_exception_then_give_up_partition() {

        when(sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE)).thenReturn(Optional.of(dataFilePartition));

        DataFileScheduler objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            // MockedStatic needs to be created on the same thread it's used
            try (MockedStatic<DataFileLoader> dataFileLoaderMockedStatic = mockStatic(DataFileLoader.class)) {
                DataFileLoader dataFileLoader = mock(DataFileLoader.class);
                dataFileLoaderMockedStatic.when(() -> DataFileLoader.create(eq(dataFilePartition), any(InputCodec.class),
                                any(Buffer.class), any(S3ObjectReader.class),
                                any(ExportRecordConverter.class), any(PluginMetrics.class),
                                any(EnhancedSourceCoordinator.class), any(), any(Duration.class)))
                        .thenReturn(dataFileLoader);
                doThrow(new RuntimeException()).when(dataFileLoader).run();
                objectUnderTest.run();
            }
        });
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).giveUpPartition(dataFilePartition));
        executorService.shutdownNow();

        verify(exportFileSuccessCounter, never()).increment();
        verify(exportFileErrorCounter).increment();
        verify(sourceCoordinator).giveUpPartition(dataFilePartition);
    }

    @Test
    void test_shutdown() {
        DataFileScheduler objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(objectUnderTest);

        objectUnderTest.shutdown();

        verifyNoMoreInteractions(sourceCoordinator);
        executorService.shutdownNow();
    }

    private DataFileScheduler createObjectUnderTest() {
        return new DataFileScheduler(sourceCoordinator, sourceConfig, s3Prefix, s3Client, eventFactory, buffer, pluginMetrics, acknowledgementSetManager);
    }
}