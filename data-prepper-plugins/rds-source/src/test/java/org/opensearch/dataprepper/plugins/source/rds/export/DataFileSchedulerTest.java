/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.model.LoadStatus;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private HeadlessPipeline failurePipeline;

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
        mockDbTableMetadata();
        executorService.submit(() -> {
                    // MockedStatic needs to be created on the same thread it's used
                    try (MockedStatic<DataFileLoader> dataFileLoaderMockedStatic = mockStatic(DataFileLoader.class)) {
                        DataFileLoader dataFileLoader = mock(DataFileLoader.class);
                        dataFileLoaderMockedStatic.when(() -> DataFileLoader.create(eq(dataFilePartition), any(InputCodec.class),
                                        any(Buffer.class), any(S3ObjectReader.class),
                                        any(ExportRecordConverter.class), any(PluginMetrics.class),
                                        any(EnhancedSourceCoordinator.class), any(), any(Duration.class), any(DbTableMetadata.class),
                                        any(PluginSetting.class), any(PipelineDescription.class), any(HeadlessPipeline.class)))
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
        mockDbTableMetadata();
        executorService.submit(() -> {
            // MockedStatic needs to be created on the same thread it's used
            try (MockedStatic<DataFileLoader> dataFileLoaderMockedStatic = mockStatic(DataFileLoader.class)) {
                DataFileLoader dataFileLoader = mock(DataFileLoader.class);
                dataFileLoaderMockedStatic.when(() -> DataFileLoader.create(eq(dataFilePartition), any(InputCodec.class),
                                any(Buffer.class), any(S3ObjectReader.class),
                                any(ExportRecordConverter.class), any(PluginMetrics.class),
                                any(EnhancedSourceCoordinator.class), any(), any(Duration.class), any(DbTableMetadata.class),
                                any(PluginSetting.class), any(PipelineDescription.class), any(HeadlessPipeline.class)))
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
    void test_when_getPartition_initially_returns_empty_then_retries_and_updates_load_status() {
        final String exportTaskId = UUID.randomUUID().toString();

        final GlobalState loadStatusGlobalState = mock(GlobalState.class);
        final int totalFiles = 5;
        final Map<String, Object> loadStatusMap = new LoadStatus(totalFiles, totalFiles - 2).toMap();
        when(loadStatusGlobalState.getProgressState()).thenReturn(Optional.of(loadStatusMap));

        // First call returns empty (transient failure), second call returns valid state
        when(sourceCoordinator.getPartition(exportTaskId))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(loadStatusGlobalState));

        createObjectUnderTest().updateLoadStatus(exportTaskId, Duration.ofSeconds(2));

        verify(sourceCoordinator).saveProgressStateForPartition(any(GlobalState.class), isNull());
    }

    @Test
    void test_when_last_file_loaded_with_stream_enabled_then_creates_stream_trigger_partition() {
        final String exportTaskId = UUID.randomUUID().toString();
        final String dbIdentifier = UUID.randomUUID().toString();
        when(sourceConfig.isStreamEnabled()).thenReturn(true);
        when(sourceConfig.getDbIdentifier()).thenReturn(dbIdentifier);

        final GlobalState loadStatusGlobalState = mock(GlobalState.class);
        final int totalFiles = 3;
        final Map<String, Object> loadStatusMap = new LoadStatus(totalFiles, totalFiles - 1).toMap();
        when(loadStatusGlobalState.getProgressState()).thenReturn(Optional.of(loadStatusMap));
        when(sourceCoordinator.getPartition(exportTaskId)).thenReturn(Optional.of(loadStatusGlobalState));

        createObjectUnderTest().updateLoadStatus(exportTaskId, Duration.ofSeconds(2));

        verify(sourceCoordinator).createPartition(any(GlobalState.class));
    }

    @Test
    void test_when_createPartition_throws_transiently_then_retries_and_partition_is_completed() {
        final String exportTaskId = UUID.randomUUID().toString();
        final String dbIdentifier = UUID.randomUUID().toString();
        when(sourceConfig.isStreamEnabled()).thenReturn(true);
        when(sourceConfig.getDbIdentifier()).thenReturn(dbIdentifier);

        final GlobalState loadStatusGlobalState = mock(GlobalState.class);
        final int totalFiles = 3;
        final Map<String, Object> loadStatusMap = new LoadStatus(totalFiles, totalFiles - 1).toMap();
        when(loadStatusGlobalState.getProgressState()).thenReturn(Optional.of(loadStatusMap));
        when(sourceCoordinator.getPartition(exportTaskId)).thenReturn(Optional.of(loadStatusGlobalState));

        // createPartition throws once then succeeds — verifies retry behavior
        when(sourceCoordinator.createPartition(any(GlobalState.class)))
                .thenThrow(new RuntimeException("DynamoDB error"))
                .thenReturn(true);

        createObjectUnderTest().updateLoadStatus(exportTaskId, Duration.ofSeconds(2));

        // saveProgressStateForPartition must be called exactly once — no retry/overshoot
        verify(sourceCoordinator, times(1)).saveProgressStateForPartition(any(GlobalState.class), isNull());
        // createPartition retried: first threw, second succeeded
        verify(sourceCoordinator, times(2)).createPartition(any(GlobalState.class));
    }

    @Test
    void test_given_acknowledgments_enabled_when_data_file_is_acknowledged_then_active_worker_count_returns_to_zero() {
        final String exportTaskId = UUID.randomUUID().toString();
        when(dataFilePartition.getExportTaskId()).thenReturn(exportTaskId);
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(true);
        when(sourceConfig.getDataFileAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(30));
        when(sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE))
                .thenReturn(Optional.of(dataFilePartition))
                .thenReturn(Optional.empty());
        mockLoadStatus(exportTaskId);
        final List<Consumer<Boolean>> ackCallbacks = captureAcknowledgementCallbacks();

        final DataFileScheduler objectUnderTest = createObjectUnderTest();
        final AtomicInteger numOfWorkers = captureActiveWorkerCount();
        final DataFileLoader dataFileLoader = mock(DataFileLoader.class);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        mockDbTableMetadata();
        executorService.submit(() -> {
            // MockedStatic needs to be created on the same thread it's used
            try (MockedStatic<DataFileLoader> dataFileLoaderMockedStatic = mockStatic(DataFileLoader.class)) {
                mockDataFileLoaderCreation(dataFileLoaderMockedStatic, dataFilePartition, dataFileLoader);
                objectUnderTest.run();
            }
        });

        // The loader finishes before the sink acknowledges its records
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> verify(dataFileLoader).run());
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(numOfWorkers.get(), equalTo(0)));
        assertThat(ackCallbacks.size(), equalTo(1));

        ackCallbacks.get(0).accept(true);

        verify(sourceCoordinator).completePartition(dataFilePartition);
        objectUnderTest.shutdown();
        executorService.shutdownNow();

        assertThat("active worker count after one data file was loaded and acknowledged",
                numOfWorkers.get(), equalTo(0));
    }

    @Test
    void test_given_acknowledged_data_file_then_does_not_acquire_partition_while_loader_is_busy() {
        final String exportTaskId = UUID.randomUUID().toString();
        when(dataFilePartition.getExportTaskId()).thenReturn(exportTaskId);
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(true);
        when(sourceConfig.getDataFileAcknowledgmentTimeout()).thenReturn(Duration.ofMinutes(30));
        mockLoadStatus(exportTaskId);
        final List<Consumer<Boolean>> ackCallbacks = captureAcknowledgementCallbacks();

        final DataFilePartition longRunningPartition = mock(DataFilePartition.class);
        final DataFilePartition waitingPartition = mock(DataFilePartition.class);
        when(sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE))
                .thenReturn(Optional.of(dataFilePartition))
                .thenReturn(Optional.of(longRunningPartition))
                .thenReturn(Optional.of(waitingPartition))
                .thenReturn(Optional.empty());

        // The first data file is acknowledged by the sink as soon as it is loaded
        final DataFileLoader acknowledgedLoader = mock(DataFileLoader.class);
        doAnswer(invocation -> {
            ackCallbacks.get(0).accept(true);
            return null;
        }).when(acknowledgedLoader).run();

        // The second data file keeps the only loader thread busy
        final CountDownLatch longRunningLoaderRelease = new CountDownLatch(1);
        final DataFileLoader longRunningLoader = mock(DataFileLoader.class);
        doAnswer(invocation -> {
            longRunningLoaderRelease.await();
            return null;
        }).when(longRunningLoader).run();

        final DataFileLoader waitingLoader = mock(DataFileLoader.class);

        final DataFileScheduler objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        mockDbTableMetadata();
        executorService.submit(() -> {
            // MockedStatic needs to be created on the same thread it's used
            try (MockedStatic<DataFileLoader> dataFileLoaderMockedStatic = mockStatic(DataFileLoader.class)) {
                mockDataFileLoaderCreation(dataFileLoaderMockedStatic, dataFilePartition, acknowledgedLoader);
                mockDataFileLoaderCreation(dataFileLoaderMockedStatic, longRunningPartition, longRunningLoader);
                mockDataFileLoaderCreation(dataFileLoaderMockedStatic, waitingPartition, waitingLoader);
                objectUnderTest.run();
            }
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(longRunningLoader).run());

        // The scheduler polls every 2 seconds: the waiting partition must not be acquired while the loader is busy
        await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> verify(sourceCoordinator, times(2)).acquireAvailablePartition(DataFilePartition.PARTITION_TYPE));

        longRunningLoaderRelease.countDown();
        objectUnderTest.shutdown();
        executorService.shutdownNow();
    }

    @Disabled("Flaky test, needs to be fixed")
    @Test
    void test_shutdown() throws InterruptedException {
        DataFileScheduler objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(objectUnderTest);

        objectUnderTest.shutdown();
        Thread.sleep(100);

        verifyNoMoreInteractions(sourceCoordinator);
        executorService.shutdownNow();
    }

    private DataFileScheduler createObjectUnderTest() {
        return new DataFileScheduler(sourceCoordinator, sourceConfig, s3Prefix, s3Client, eventFactory, buffer, pluginMetrics, acknowledgementSetManager, pluginSetting, pipelineDescription, failurePipeline);
    }

    private void mockDbTableMetadata() {
        final Map<String, Map<String, String>> tableColumnDataTypeMap = new HashMap<>();
        final Map<String, String> columnDataTypeMap = new HashMap<>();
        columnDataTypeMap.put("int_column", "INTEGER");
        final String tableName = UUID.randomUUID().toString();
        tableColumnDataTypeMap.put(tableName, columnDataTypeMap);

        final String dbIdentifier = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final int port = new Random().nextInt();

        final DbMetadata dbMetadata = DbMetadata.builder()
                .dbIdentifier(dbIdentifier)
                .endpoint(hostName)
                .port(port)
                .build();
        final Map<String, Object> map = new HashMap<>();
        map.put("dbMetadata", dbMetadata.toMap());
        map.put("tableColumnDataTypeMap", tableColumnDataTypeMap);
        final GlobalState globalStatePartition = mock(GlobalState.class);
        when(sourceCoordinator.getPartition(sourceConfig.getDbIdentifier())).thenReturn(Optional.of(globalStatePartition));
        when(globalStatePartition.getProgressState()).thenReturn(Optional.of(map));
    }

    private void mockDataFileLoaderCreation(final MockedStatic<DataFileLoader> dataFileLoaderMockedStatic,
                                            final DataFilePartition partition,
                                            final DataFileLoader dataFileLoader) {
        dataFileLoaderMockedStatic.when(() -> DataFileLoader.create(eq(partition), any(InputCodec.class),
                        any(Buffer.class), any(S3ObjectReader.class),
                        any(ExportRecordConverter.class), any(PluginMetrics.class),
                        any(EnhancedSourceCoordinator.class), any(), any(Duration.class), any(DbTableMetadata.class),
                        any(PluginSetting.class), any(PipelineDescription.class), any(HeadlessPipeline.class)))
                .thenReturn(dataFileLoader);
    }

    private void mockLoadStatus(final String exportTaskId) {
        final GlobalState loadStatusGlobalState = mock(GlobalState.class);
        final Map<String, Object> loadStatusMap = new LoadStatus(10, 0).toMap();
        when(loadStatusGlobalState.getProgressState()).thenReturn(Optional.of(loadStatusMap));
        when(sourceCoordinator.getPartition(exportTaskId)).thenReturn(Optional.of(loadStatusGlobalState));
    }

    private List<Consumer<Boolean>> captureAcknowledgementCallbacks() {
        final List<Consumer<Boolean>> callbacks = new CopyOnWriteArrayList<>();
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenAnswer(invocation -> {
            callbacks.add(invocation.getArgument(0));
            return mock(AcknowledgementSet.class);
        });
        return callbacks;
    }

    private AtomicInteger captureActiveWorkerCount() {
        // The gauge is registered on the scheduler's own worker counter
        final ArgumentCaptor<AtomicInteger> captor = ArgumentCaptor.forClass(AtomicInteger.class);
        verify(pluginMetrics).gauge(eq(ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE), captor.capture(), any());
        return captor.getValue();
    }
}
