/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
    private DataFilePartition dataFilePartition;

    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
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

        verifyNoInteractions(s3Client, buffer);
    }

    @Test
    void test_given_available_datafile_partition_then_load_datafile() {
        DataFileScheduler objectUnderTest = createObjectUnderTest();
        final String exportTaskId = UUID.randomUUID().toString();
        when(dataFilePartition.getExportTaskId()).thenReturn(exportTaskId);

        when(sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE)).thenReturn(Optional.of(dataFilePartition));
        final GlobalState globalStatePartition = mock(GlobalState.class);
        final int totalFiles = random.nextInt() + 1;
        final Map<String, Object> loadStatusMap = new LoadStatus(totalFiles, totalFiles - 1).toMap();
        when(globalStatePartition.getProgressState()).thenReturn(Optional.of(loadStatusMap));
        when(sourceCoordinator.getPartition(exportTaskId)).thenReturn(Optional.of(globalStatePartition));

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
                    // MockedStatic needs to be created on the same thread it's used
                    try (MockedStatic<DataFileLoader> dataFileLoaderMockedStatic = mockStatic(DataFileLoader.class)) {
                        DataFileLoader dataFileLoader = mock(DataFileLoader.class);
                        dataFileLoaderMockedStatic.when(() -> DataFileLoader.create(
                                eq(dataFilePartition), any(InputCodec.class), any(BufferAccumulator.class), any(S3ObjectReader.class), any(ExportRecordConverter.class)))
                                .thenReturn(dataFileLoader);
                        doNothing().when(dataFileLoader).run();
                        objectUnderTest.run();
                    }
        });
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).completePartition(dataFilePartition));
        executorService.shutdownNow();

        verify(sourceCoordinator).completePartition(dataFilePartition);
    }

    @Test
    void shutdown() {
        DataFileScheduler objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(objectUnderTest);

        objectUnderTest.shutdown();

        verifyNoMoreInteractions(sourceCoordinator);
        executorService.shutdownNow();
    }

    private DataFileScheduler createObjectUnderTest() {
        return new DataFileScheduler(sourceCoordinator, sourceConfig, s3Client, eventFactory, buffer);
    }
}