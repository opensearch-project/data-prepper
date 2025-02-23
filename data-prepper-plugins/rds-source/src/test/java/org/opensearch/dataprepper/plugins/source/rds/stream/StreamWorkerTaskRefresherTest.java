/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.resync.CascadingActionDetector;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.stream.StreamWorkerTaskRefresher.CREDENTIALS_CHANGED;
import static org.opensearch.dataprepper.plugins.source.rds.stream.StreamWorkerTaskRefresher.TASK_REFRESH_ERRORS;

@ExtendWith(MockitoExtension.class)
class StreamWorkerTaskRefresherTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private StreamPartition streamPartition;

    @Mock
    private StreamCheckpointer streamCheckpointer;

    @Mock
    private ReplicationLogClientFactory replicationLogClientFactory;

    @Mock
    private BinlogClientWrapper binaryLogClientWrapper;

    @Mock
    private BinaryLogClient binaryLogClient;

    @Mock
    private LogicalReplicationClient logicalReplicationClient;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private ExecutorService executorService;

    @Mock
    private ExecutorService newExecutorService;

    @Mock
    private Supplier<ExecutorService> executorServiceSupplier;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private StreamWorker streamWorker;

    @Mock
    private Counter credentialsChangeCounter;

    @Mock
    private Counter taskRefreshErrorsCounter;

    @Mock
    private BinlogEventListener binlogEventListener;

    @Mock
    private LogicalReplicationEventProcessor logicalReplicationEventProcessor;

    @Mock
    private DbTableMetadata dbTableMetadata;

    @Mock
    private GlobalState globalState;

    private StreamWorkerTaskRefresher streamWorkerTaskRefresher;

    @Nested
    class TestForMySql {
        @BeforeEach
        void setUp() {
            when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
            when(pluginMetrics.counter(TASK_REFRESH_ERRORS)).thenReturn(taskRefreshErrorsCounter);
            when(executorServiceSupplier.get()).thenReturn(executorService).thenReturn(newExecutorService);
            when(sourceConfig.getEngine()).thenReturn(EngineType.MYSQL);
            streamWorkerTaskRefresher = createObjectUnderTest();
        }

        @Test
        void test_initialize_then_process_stream() {
            when(replicationLogClientFactory.create(streamPartition)).thenReturn(binaryLogClientWrapper);
            when(binaryLogClientWrapper.getBinlogClient()).thenReturn(binaryLogClient);
            final Map<String, Object> progressState = mockGlobalStateAndProgressState();
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
                 MockedStatic<BinlogEventListener> binlogEventListenerMockedStatic = mockStatic(BinlogEventListener.class);
                 MockedStatic<DbTableMetadata> dbTableMetadataMockedStatic = mockStatic(DbTableMetadata.class)) {
                dbTableMetadataMockedStatic.when(() -> DbTableMetadata.fromMap(progressState)).thenReturn(dbTableMetadata);
                streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(ReplicationLogClient.class), eq(pluginMetrics)))
                        .thenReturn(streamWorker);
                binlogEventListenerMockedStatic.when(() -> BinlogEventListener.create(eq(streamPartition), eq(buffer), any(RdsSourceConfig.class),
                                any(String.class), eq(pluginMetrics), eq(binaryLogClient), eq(streamCheckpointer),
                                eq(acknowledgementSetManager), eq(dbTableMetadata), any(CascadingActionDetector.class)))
                        .thenReturn(binlogEventListener);
                streamWorkerTaskRefresher.initialize(sourceConfig);
            }

            verify(replicationLogClientFactory).create(streamPartition);
            verify(binaryLogClient).registerEventListener(binlogEventListener);

            ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(executorService).submit(runnableArgumentCaptor.capture());

            Runnable capturedRunnable = runnableArgumentCaptor.getValue();
            capturedRunnable.run();
            verify(streamWorker).processStream(streamPartition);
        }

        @Test
        void test_update_when_credentials_changed_then_refresh_task() {
            final String username = UUID.randomUUID().toString();
            final String password = UUID.randomUUID().toString();
            when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
            when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

            RdsSourceConfig sourceConfig2 = mock(RdsSourceConfig.class, RETURNS_DEEP_STUBS);
            final String password2 = UUID.randomUUID().toString();
            when(sourceConfig2.getAuthenticationConfig().getUsername()).thenReturn(username);
            when(sourceConfig2.getAuthenticationConfig().getPassword()).thenReturn(password2);
            when(sourceConfig2.getEngine()).thenReturn(EngineType.MYSQL);

            when(replicationLogClientFactory.create(streamPartition)).thenReturn(binaryLogClientWrapper).thenReturn(binaryLogClientWrapper);
            when(binaryLogClientWrapper.getBinlogClient()).thenReturn(binaryLogClient);
            final Map<String, Object> progressState = mockGlobalStateAndProgressState();
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
                 MockedStatic<BinlogEventListener> binlogEventListenerMockedStatic = mockStatic(BinlogEventListener.class);
                 MockedStatic<DbTableMetadata> dbTableMetadataMockedStatic = mockStatic(DbTableMetadata.class)) {
                dbTableMetadataMockedStatic.when(() -> DbTableMetadata.fromMap(progressState)).thenReturn(dbTableMetadata);
                streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(ReplicationLogClient.class), eq(pluginMetrics)))
                        .thenReturn(streamWorker);
                binlogEventListenerMockedStatic.when(() -> BinlogEventListener.create(eq(streamPartition), eq(buffer), any(RdsSourceConfig.class),
                                any(String.class), eq(pluginMetrics), eq(binaryLogClient), eq(streamCheckpointer),
                                eq(acknowledgementSetManager), eq(dbTableMetadata), any(CascadingActionDetector.class)))
                        .thenReturn(binlogEventListener);
                streamWorkerTaskRefresher.initialize(sourceConfig);
                streamWorkerTaskRefresher.update(sourceConfig2);
            }

            verify(credentialsChangeCounter).increment();
            verify(executorService).shutdownNow();

            verify(replicationLogClientFactory, times(2)).create(streamPartition);
            verify(binaryLogClient, times(2)).registerEventListener(binlogEventListener);

            ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(newExecutorService).submit(runnableArgumentCaptor.capture());

            Runnable capturedRunnable = runnableArgumentCaptor.getValue();
            capturedRunnable.run();
            verify(streamWorker).processStream(streamPartition);
        }

        @Test
        void test_update_when_credentials_unchanged_then_do_nothing() {
            final String username = UUID.randomUUID().toString();
            final String password = UUID.randomUUID().toString();
            when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
            when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

            when(replicationLogClientFactory.create(streamPartition)).thenReturn(binaryLogClientWrapper);
            when(binaryLogClientWrapper.getBinlogClient()).thenReturn(binaryLogClient);
            final Map<String, Object> progressState = mockGlobalStateAndProgressState();
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
                 MockedStatic<BinlogEventListener> binlogEventListenerMockedStatic = mockStatic(BinlogEventListener.class);
                 MockedStatic<DbTableMetadata> dbTableMetadataMockedStatic = mockStatic(DbTableMetadata.class)) {
                dbTableMetadataMockedStatic.when(() -> DbTableMetadata.fromMap(progressState)).thenReturn(dbTableMetadata);
                streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(ReplicationLogClient.class), eq(pluginMetrics)))
                        .thenReturn(streamWorker);
                binlogEventListenerMockedStatic.when(() -> BinlogEventListener.create(eq(streamPartition), eq(buffer), any(RdsSourceConfig.class),
                                any(String.class), eq(pluginMetrics), eq(binaryLogClient), eq(streamCheckpointer),
                                eq(acknowledgementSetManager), eq(dbTableMetadata), any(CascadingActionDetector.class)))
                        .thenReturn(binlogEventListener);
                streamWorkerTaskRefresher.initialize(sourceConfig);
                streamWorkerTaskRefresher.update(sourceConfig);
            }

            verify(credentialsChangeCounter, never()).increment();
            verify(executorService, never()).shutdownNow();
        }

        @Test
        void test_shutdown() {
            streamWorkerTaskRefresher.shutdown();
            verify(executorService).shutdownNow();
        }

        private StreamWorkerTaskRefresher createObjectUnderTest() {
            final String s3Prefix = UUID.randomUUID().toString();

            return new StreamWorkerTaskRefresher(
                    sourceCoordinator, streamPartition, streamCheckpointer, s3Prefix, replicationLogClientFactory, buffer,
                    executorServiceSupplier, acknowledgementSetManager, pluginMetrics);
        }
    }

    @Nested
    class TestForPostgres {
        @BeforeEach
        void setUp() {
            when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
            when(pluginMetrics.counter(TASK_REFRESH_ERRORS)).thenReturn(taskRefreshErrorsCounter);
            when(executorServiceSupplier.get()).thenReturn(executorService).thenReturn(newExecutorService);
            when(sourceConfig.getEngine()).thenReturn(EngineType.POSTGRES);
            streamWorkerTaskRefresher = createObjectUnderTest();
        }

        @Test
        void test_initialize_then_process_stream() {
            when(replicationLogClientFactory.create(streamPartition)).thenReturn(logicalReplicationClient);
            mockGlobalStateAndProgressState();
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
                 MockedStatic<LogicalReplicationEventProcessor> logicalReplicationEventProcessorMockedStatic = mockStatic(LogicalReplicationEventProcessor.class)) {
                streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(ReplicationLogClient.class), eq(pluginMetrics)))
                        .thenReturn(streamWorker);
                logicalReplicationEventProcessorMockedStatic.when(() -> LogicalReplicationEventProcessor.create(eq(streamPartition), any(RdsSourceConfig.class),
                                eq(buffer), any(String.class), eq(pluginMetrics), eq(logicalReplicationClient), eq(streamCheckpointer), eq(acknowledgementSetManager)))
                        .thenReturn(logicalReplicationEventProcessor);
                streamWorkerTaskRefresher.initialize(sourceConfig);
            }

            verify(replicationLogClientFactory).create(streamPartition);

            ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(executorService).submit(runnableArgumentCaptor.capture());

            Runnable capturedRunnable = runnableArgumentCaptor.getValue();
            capturedRunnable.run();
            verify(streamWorker).processStream(streamPartition);
        }

        @Test
        void test_update_when_credentials_changed_then_refresh_task() {
            final String username = UUID.randomUUID().toString();
            final String password = UUID.randomUUID().toString();
            when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
            when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

            RdsSourceConfig sourceConfig2 = mock(RdsSourceConfig.class, RETURNS_DEEP_STUBS);
            final String password2 = UUID.randomUUID().toString();
            when(sourceConfig2.getAuthenticationConfig().getUsername()).thenReturn(username);
            when(sourceConfig2.getAuthenticationConfig().getPassword()).thenReturn(password2);
            when(sourceConfig2.getEngine()).thenReturn(EngineType.POSTGRES);

            when(replicationLogClientFactory.create(streamPartition)).thenReturn(logicalReplicationClient);
            mockGlobalStateAndProgressState();
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
                 MockedStatic<LogicalReplicationEventProcessor> logicalReplicationEventProcessorMockedStatic = mockStatic(LogicalReplicationEventProcessor.class)) {
                streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(ReplicationLogClient.class), eq(pluginMetrics)))
                        .thenReturn(streamWorker);
                logicalReplicationEventProcessorMockedStatic.when(() -> LogicalReplicationEventProcessor.create(eq(streamPartition), any(RdsSourceConfig.class),
                                eq(buffer), any(String.class), eq(pluginMetrics), eq(logicalReplicationClient), eq(streamCheckpointer), eq(acknowledgementSetManager)))
                        .thenReturn(logicalReplicationEventProcessor);
                streamWorkerTaskRefresher.initialize(sourceConfig);
                streamWorkerTaskRefresher.update(sourceConfig2);
            }

            verify(credentialsChangeCounter).increment();
            verify(executorService).shutdownNow();

            verify(replicationLogClientFactory, times(2)).create(streamPartition);

            ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
            verify(newExecutorService).submit(runnableArgumentCaptor.capture());

            Runnable capturedRunnable = runnableArgumentCaptor.getValue();
            capturedRunnable.run();
            verify(streamWorker).processStream(streamPartition);
        }

        @Test
        void test_update_when_credentials_unchanged_then_do_nothing() {
            final String username = UUID.randomUUID().toString();
            final String password = UUID.randomUUID().toString();
            when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
            when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

            when(replicationLogClientFactory.create(streamPartition)).thenReturn(logicalReplicationClient);
            mockGlobalStateAndProgressState();
            try (MockedStatic<StreamWorker> streamWorkerMockedStatic = mockStatic(StreamWorker.class);
                 MockedStatic<LogicalReplicationEventProcessor> logicalReplicationEventProcessorMockedStatic = mockStatic(LogicalReplicationEventProcessor.class)) {
                streamWorkerMockedStatic.when(() -> StreamWorker.create(eq(sourceCoordinator), any(ReplicationLogClient.class), eq(pluginMetrics)))
                        .thenReturn(streamWorker);
                logicalReplicationEventProcessorMockedStatic.when(() -> LogicalReplicationEventProcessor.create(eq(streamPartition), any(RdsSourceConfig.class),
                                eq(buffer), any(String.class), eq(pluginMetrics), eq(logicalReplicationClient), eq(streamCheckpointer), eq(acknowledgementSetManager)))
                        .thenReturn(logicalReplicationEventProcessor);
                streamWorkerTaskRefresher.initialize(sourceConfig);
                streamWorkerTaskRefresher.update(sourceConfig);
            }

            verify(credentialsChangeCounter, never()).increment();
            verify(executorService, never()).shutdownNow();
        }

        @Test
        void test_shutdown() {
            streamWorkerTaskRefresher.shutdown();
            verify(executorService).shutdownNow();
        }

        private StreamWorkerTaskRefresher createObjectUnderTest() {
            final String s3Prefix = UUID.randomUUID().toString();

            return new StreamWorkerTaskRefresher(
                    sourceCoordinator, streamPartition, streamCheckpointer, s3Prefix, replicationLogClientFactory, buffer,
                    executorServiceSupplier, acknowledgementSetManager, pluginMetrics);
        }
    }

    private Map<String, Object> mockGlobalStateAndProgressState() {
        final String dbIdentifier = UUID.randomUUID().toString();
        when(streamPartition.getPartitionKey()).thenReturn(dbIdentifier);
        when(sourceCoordinator.getPartition(dbIdentifier)).thenReturn(Optional.of(globalState));
        final Map<String, Object> progressState = getDbTableMetaDataMap();
        when(globalState.getProgressState()).thenReturn(Optional.of(progressState));
        return progressState;
    }

    private Map<String, Object> getDbTableMetaDataMap() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final int port = new Random().nextInt();
        final String tableName = UUID.randomUUID().toString();

        final DbMetadata dbMetadata = DbMetadata.builder()
                .dbIdentifier(dbIdentifier)
                .endpoint(hostName)
                .port(port)
                .build();
        final Map<String, Map<String, String>> tableColumnDataTypeMap = new HashMap<>();
        final Map<String, String> columnDataTypeMap = new HashMap<>();
        columnDataTypeMap.put("int_column", "INTEGER");
        tableColumnDataTypeMap.put(tableName, columnDataTypeMap);


        final Map<String, Object> map = new HashMap<>();
        map.put("dbMetadata", dbMetadata.toMap());
        map.put("tableColumnDataTypeMap", tableColumnDataTypeMap);

        return map;
    }
}
