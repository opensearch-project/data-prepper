/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.configuration.TableFilterConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.TlsConfig;
import org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.rds.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.rds.resync.ResyncScheduler;
import org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager;
import org.opensearch.dataprepper.plugins.source.rds.stream.StreamScheduler;
import org.opensearch.dataprepper.plugins.source.rds.utils.IdentifierShortener;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.Endpoint;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.RdsService.MAX_SOURCE_IDENTIFIER_LENGTH;
import static org.opensearch.dataprepper.plugins.source.rds.RdsService.S3_PATH_DELIMITER;

@ExtendWith(MockitoExtension.class)
class RdsServiceTest {

    @Mock
    private RdsClient rdsClient;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private ExecutorService executor;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private EventFactory eventFactory;

    @Mock
    private ClientFactory clientFactory;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @BeforeEach
    void setUp() {
        when(clientFactory.buildRdsClient()).thenReturn(rdsClient);
        when(sourceConfig.getEngine()).thenReturn(EngineType.MYSQL);
    }

    @Test
    void test_normal_service_start_when_export_is_enabled() {
        prepareMocks();

        when(sourceConfig.isExportEnabled()).thenReturn(true);
        when(sourceConfig.isStreamEnabled()).thenReturn(false);

        final RdsService rdsService = createObjectUnderTest();
        final RdsService spyRdsService = spy(rdsService);

        doReturn(schemaManager).when(spyRdsService).getSchemaManager(any(), any());

        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            spyRdsService.start(buffer);
        }

        verify(executor).submit(any(LeaderScheduler.class));
        verify(executor).submit(any(ExportScheduler.class));
        verify(executor).submit(any(DataFileScheduler.class));
        verify(executor, never()).submit(any(StreamScheduler.class));
    }

    @Test
    void test_normal_service_start_when_stream_is_enabled() {
        prepareMocks();

        when(sourceConfig.isExportEnabled()).thenReturn(false);
        when(sourceConfig.isStreamEnabled()).thenReturn(true);

        final RdsSourceConfig.AuthenticationConfig authConfig = mock(RdsSourceConfig.AuthenticationConfig.class);
        when(authConfig.getUsername()).thenReturn(UUID.randomUUID().toString());
        when(authConfig.getPassword()).thenReturn(UUID.randomUUID().toString());
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authConfig);
        when(sourceConfig.getTlsConfig()).thenReturn(mock(TlsConfig.class));

        final String s3Prefix = UUID.randomUUID().toString();
        final String partitionPrefix = UUID.randomUUID().toString();
        when(sourceConfig.getS3Prefix()).thenReturn(s3Prefix);
        when(sourceCoordinator.getPartitionPrefix()).thenReturn(partitionPrefix);

        final RdsService rdsService = createObjectUnderTest();
        final RdsService spyRdsService = spy(rdsService);

        doReturn(schemaManager).when(spyRdsService).getSchemaManager(any(), any());
        final String[] s3PrefixArray = new String[1];

        final BackgroundThreadFactory threadFactory = mock(BackgroundThreadFactory.class);
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedConstruction<LeaderScheduler> leaderSchedulerMockedConstruction = mockConstruction(LeaderScheduler.class,
                     (mock, context) -> s3PrefixArray[0] = (String) context.arguments().get(2));
             final MockedStatic<BackgroundThreadFactory> backgroundThreadFactoryMockedStatic = mockStatic(BackgroundThreadFactory.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            backgroundThreadFactoryMockedStatic.when(() -> BackgroundThreadFactory.defaultExecutorThreadFactory(any())).thenReturn(threadFactory);
            spyRdsService.start(buffer);
        }

        assertThat(s3PrefixArray[0], equalTo(s3Prefix + S3_PATH_DELIMITER + IdentifierShortener.shortenIdentifier(partitionPrefix, MAX_SOURCE_IDENTIFIER_LENGTH)));
        verify(executor).submit(any(LeaderScheduler.class));
        verify(executor).submit(any(StreamScheduler.class));
        verify(executor).submit(any(ResyncScheduler.class));
        verify(executor, never()).submit(any(ExportScheduler.class));
        verify(executor, never()).submit(any(DataFileScheduler.class));
    }

    @Test
    void test_service_shutdown_calls_executor_shutdownNow() {
        prepareMocks();

        RdsService rdsService = createObjectUnderTest();
        final RdsService spyRdsService = spy(rdsService);

        doReturn(schemaManager).when(spyRdsService).getSchemaManager(any(), any());
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            spyRdsService.start(buffer);
        }
        spyRdsService.shutdown();

        verify(executor).shutdownNow();
    }

    private void prepareMocks() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final String host = UUID.randomUUID().toString();
        final int port = 3306;
        final DescribeDbInstancesResponse describeDbInstancesResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(DBInstance.builder()
                        .endpoint(Endpoint.builder()
                                .address(host)
                                .port(port)
                                .build())
                        .build())
                .build();
        final TableFilterConfig tableFilterConfig = mock(TableFilterConfig.class);
        final String databaseName = UUID.randomUUID().toString();
        final Set<String> tableNames = Set.of("database1.table1", "database2.table2");

        when(sourceConfig.getDbIdentifier()).thenReturn(dbIdentifier);
        when(sourceConfig.getTables()).thenReturn(tableFilterConfig);
        when(tableFilterConfig.getDatabase()).thenReturn(databaseName);
        when(schemaManager.getTableNames(databaseName)).thenReturn(tableNames);
        when(schemaManager.getColumnDataTypes(new ArrayList<>(tableNames))).thenReturn(mock(Map.class));
        when(rdsClient.describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);
    }

    private RdsService createObjectUnderTest() {
        return new RdsService(sourceCoordinator, sourceConfig, eventFactory, clientFactory, pluginMetrics, acknowledgementSetManager, pluginConfigObservable);
    }
}