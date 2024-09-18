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
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.configuration.TlsConfig;
import org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.rds.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.rds.stream.StreamScheduler;
import org.opensearch.dataprepper.plugins.source.rds.utils.IdentifierShortener;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.Endpoint;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
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
    private EventFactory eventFactory;

    @Mock
    private ClientFactory clientFactory;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @BeforeEach
    void setUp() {
        when(clientFactory.buildRdsClient()).thenReturn(rdsClient);
    }

    @Test
    void test_normal_service_start_when_export_is_enabled() {
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
        when(sourceConfig.isExportEnabled()).thenReturn(true);
        when(sourceConfig.isStreamEnabled()).thenReturn(false);
        when(sourceConfig.getDbIdentifier()).thenReturn(dbIdentifier);
        when(rdsClient.describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);

        final RdsService rdsService = createObjectUnderTest();
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            rdsService.start(buffer);
        }

        verify(executor).submit(any(LeaderScheduler.class));
        verify(executor).submit(any(ExportScheduler.class));
        verify(executor).submit(any(DataFileScheduler.class));
        verify(executor, never()).submit(any(StreamScheduler.class));
    }

    @Test
    void test_normal_service_start_when_stream_is_enabled() {
        when(sourceConfig.isStreamEnabled()).thenReturn(true);
        when(sourceConfig.isExportEnabled()).thenReturn(false);
        final String dbIdentifier = UUID.randomUUID().toString();
        when(sourceConfig.getDbIdentifier()).thenReturn(dbIdentifier);
        final DescribeDbInstancesResponse describeDbInstancesResponse = mock(DescribeDbInstancesResponse.class, RETURNS_DEEP_STUBS);
        final Endpoint hostEndpoint = Endpoint.builder()
                .address(UUID.randomUUID().toString())
                .port(3306)
                .build();
        when(describeDbInstancesResponse.dbInstances().get(0)).thenReturn(
                DBInstance.builder()
                        .endpoint(hostEndpoint)
                        .build());
        when(rdsClient.describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);

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
        final String[] s3PrefixArray = new String[1];
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedConstruction<LeaderScheduler> leaderSchedulerMockedConstruction = mockConstruction(LeaderScheduler.class,
                     (mock, context) -> s3PrefixArray[0] = (String) context.arguments().get(2))) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            rdsService.start(buffer);
        }

        assertThat(s3PrefixArray[0], equalTo(s3Prefix + S3_PATH_DELIMITER + IdentifierShortener.shortenIdentifier(partitionPrefix, MAX_SOURCE_IDENTIFIER_LENGTH)));
        verify(executor).submit(any(LeaderScheduler.class));
        verify(executor).submit(any(StreamScheduler.class));
        verify(executor, never()).submit(any(ExportScheduler.class));
        verify(executor, never()).submit(any(DataFileScheduler.class));
    }

    @Test
    void test_service_shutdown_calls_executor_shutdownNow() {
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
        when(sourceConfig.getDbIdentifier()).thenReturn(dbIdentifier);
        when(rdsClient.describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);

        RdsService rdsService = createObjectUnderTest();
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            rdsService.start(buffer);
        }
        rdsService.shutdown();

        verify(executor).shutdownNow();
    }

    private RdsService createObjectUnderTest() {
        return new RdsService(sourceCoordinator, sourceConfig, eventFactory, clientFactory, pluginMetrics, acknowledgementSetManager);
    }
}