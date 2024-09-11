/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfig;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfigSupplier;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseCoordinationTableConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.ConsumerStrategy;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamPollingConfig;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsLevel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KinesisServiceTest {
    private final String PIPELINE_NAME = "kinesis-pipeline-test";
    private final String streamId = "stream-1";

    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMillis(0);
    private static final int NUMBER_OF_RECORDS_TO_ACCUMULATE = 10;
    private static final int DEFAULT_MAX_RECORDS = 10000;
    private static final int IDLE_TIME_BETWEEN_READS_IN_MILLIS = 250;
    private static final String awsAccountId = "123456789012";
    private static final String streamArnFormat = "arn:aws:kinesis:us-east-1:%s:stream/%s";
    private static final Instant streamCreationTime = Instant.now();

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private KinesisSourceConfig kinesisSourceConfig;

    @Mock
    private KinesisStreamConfig kinesisStreamConfig;

    @Mock
    private KinesisStreamPollingConfig kinesisStreamPollingConfig;

    @Mock
    private AwsAuthenticationConfig awsAuthenticationConfig;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private KinesisClientFactory kinesisClientFactory;

    @Mock
    private KinesisAsyncClient kinesisClient;

    @Mock
    private DynamoDbAsyncClient dynamoDbClient;

    @Mock
    private CloudWatchAsyncClient cloudWatchClient;

    @Mock
    Buffer<Record<Event>> buffer;

    @Mock
    private Scheduler scheduler;

    @Mock
    KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier;

    @Mock
    KinesisLeaseConfig kinesisLeaseConfig;

    @Mock
    KinesisLeaseCoordinationTableConfig kinesisLeaseCoordinationTableConfig;

    @Mock
    WorkerIdentifierGenerator workerIdentifierGenerator;

    @BeforeEach
    void setup() {
        awsAuthenticationConfig = mock(AwsAuthenticationConfig.class);
        kinesisSourceConfig = mock(KinesisSourceConfig.class);
        kinesisStreamConfig = mock(KinesisStreamConfig.class);
        kinesisStreamPollingConfig = mock(KinesisStreamPollingConfig.class);
        kinesisClient = mock(KinesisAsyncClient.class);
        dynamoDbClient = mock(DynamoDbAsyncClient.class);
        cloudWatchClient = mock(CloudWatchAsyncClient.class);
        kinesisClientFactory = mock(KinesisClientFactory.class);
        scheduler = mock(Scheduler.class);
        pipelineDescription = mock(PipelineDescription.class);
        buffer = mock(Buffer.class);
        kinesisLeaseConfigSupplier = mock(KinesisLeaseConfigSupplier.class);
        kinesisLeaseConfig = mock(KinesisLeaseConfig.class);
        workerIdentifierGenerator = mock(WorkerIdentifierGenerator.class);
        kinesisLeaseCoordinationTableConfig = mock(KinesisLeaseCoordinationTableConfig.class);
        when(kinesisLeaseConfig.getLeaseCoordinationTable()).thenReturn(kinesisLeaseCoordinationTableConfig);
        when(kinesisLeaseCoordinationTableConfig.getTableName()).thenReturn("kinesis-lease-table");
        when(kinesisLeaseCoordinationTableConfig.getRegion()).thenReturn("us-east-1");
        when(kinesisLeaseCoordinationTableConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig()).thenReturn(Optional.ofNullable(kinesisLeaseConfig));

        when(awsAuthenticationConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(awsAuthenticationConfig.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsExternalId()).thenReturn(UUID.randomUUID().toString());
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
        StreamDescription streamDescription = StreamDescription.builder()
                .streamARN(String.format(streamArnFormat, awsAccountId, streamId))
                .streamCreationTimestamp(streamCreationTime)
                .streamName(streamId)
                .build();

        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamId)
                .build();

        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
                .streamDescription(streamDescription)
                .build();

        when(kinesisClient.describeStream(describeStreamRequest)).thenReturn(CompletableFuture.completedFuture(describeStreamResponse));

        when(kinesisSourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
        when(kinesisStreamConfig.getName()).thenReturn(streamId);
        when(kinesisStreamConfig.getCheckPointInterval()).thenReturn(CHECKPOINT_INTERVAL);
        when(kinesisStreamConfig.getInitialPosition()).thenReturn(InitialPositionInStream.LATEST);
        when(kinesisSourceConfig.getConsumerStrategy()).thenReturn(ConsumerStrategy.ENHANCED_FAN_OUT);
        when(kinesisSourceConfig.getPollingConfig()).thenReturn(kinesisStreamPollingConfig);
        when(kinesisStreamPollingConfig.getMaxPollingRecords()).thenReturn(DEFAULT_MAX_RECORDS);
        when(kinesisStreamPollingConfig.getIdleTimeBetweenReads()).thenReturn(Duration.ofMillis(IDLE_TIME_BETWEEN_READS_IN_MILLIS));

        List<KinesisStreamConfig> streamConfigs = new ArrayList<>();
        streamConfigs.add(kinesisStreamConfig);
        when(kinesisSourceConfig.getStreams()).thenReturn(streamConfigs);
        when(kinesisSourceConfig.getNumberOfRecordsToAccumulate()).thenReturn(NUMBER_OF_RECORDS_TO_ACCUMULATE);

        when(kinesisClientFactory.buildDynamoDBClient(kinesisLeaseCoordinationTableConfig.getAwsRegion())).thenReturn(dynamoDbClient);
        when(kinesisClientFactory.buildKinesisAsyncClient(awsAuthenticationConfig.getAwsRegion())).thenReturn(kinesisClient);
        when(kinesisClientFactory.buildCloudWatchAsyncClient(kinesisLeaseCoordinationTableConfig.getAwsRegion())).thenReturn(cloudWatchClient);
        when(kinesisClient.serviceClientConfiguration()).thenReturn(KinesisServiceClientConfiguration.builder().region(Region.US_EAST_1).build());
        when(scheduler.startGracefulShutdown()).thenReturn(CompletableFuture.completedFuture(true));
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(workerIdentifierGenerator.generate()).thenReturn(UUID.randomUUID().toString());
    }

    public KinesisService createObjectUnderTest() {
        return new KinesisService(kinesisSourceConfig, kinesisClientFactory, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, kinesisLeaseConfigSupplier, workerIdentifierGenerator);
    }

    @Test
    void testServiceStart() {
        KinesisService kinesisService = createObjectUnderTest();
        kinesisService.start(buffer);
        assertNotNull(kinesisService.getScheduler(buffer));
        verify(workerIdentifierGenerator, times(1)).generate();
    }

    @Test
    void testServiceThrowsWhenLeaseConfigIsInvalid() {
        when(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig()).thenReturn(Optional.empty());
        assertThrows(IllegalStateException.class, () -> new KinesisService(kinesisSourceConfig, kinesisClientFactory, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, kinesisLeaseConfigSupplier, workerIdentifierGenerator));
    }

    @Test
    void testCreateScheduler() {
        KinesisService kinesisService = new KinesisService(kinesisSourceConfig, kinesisClientFactory, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, kinesisLeaseConfigSupplier, workerIdentifierGenerator);
        Scheduler schedulerObjectUnderTest = kinesisService.createScheduler(buffer);

        assertNotNull(schedulerObjectUnderTest);
        assertNotNull(schedulerObjectUnderTest.checkpointConfig());
        assertNotNull(schedulerObjectUnderTest.leaseManagementConfig());
        assertSame(schedulerObjectUnderTest.leaseManagementConfig().initialPositionInStream().getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
        assertNotNull(schedulerObjectUnderTest.lifecycleConfig());
        assertNotNull(schedulerObjectUnderTest.metricsConfig());
        assertSame(schedulerObjectUnderTest.metricsConfig().metricsLevel(), MetricsLevel.DETAILED);
        assertNotNull(schedulerObjectUnderTest.processorConfig());
        assertNotNull(schedulerObjectUnderTest.retrievalConfig());
        verify(workerIdentifierGenerator, times(1)).generate();
    }

    @Test
    void testCreateSchedulerWithPollingStrategy() {
        when(kinesisSourceConfig.getConsumerStrategy()).thenReturn(ConsumerStrategy.POLLING);
        KinesisService kinesisService = new KinesisService(kinesisSourceConfig, kinesisClientFactory, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, kinesisLeaseConfigSupplier, workerIdentifierGenerator);
        Scheduler schedulerObjectUnderTest = kinesisService.createScheduler(buffer);

        assertNotNull(schedulerObjectUnderTest);
        assertNotNull(schedulerObjectUnderTest.checkpointConfig());
        assertNotNull(schedulerObjectUnderTest.leaseManagementConfig());
        assertSame(schedulerObjectUnderTest.leaseManagementConfig().initialPositionInStream().getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
        assertNotNull(schedulerObjectUnderTest.lifecycleConfig());
        assertNotNull(schedulerObjectUnderTest.metricsConfig());
        assertSame(schedulerObjectUnderTest.metricsConfig().metricsLevel(), MetricsLevel.DETAILED);
        assertNotNull(schedulerObjectUnderTest.processorConfig());
        assertNotNull(schedulerObjectUnderTest.retrievalConfig());
        verify(workerIdentifierGenerator, times(1)).generate();
    }


    @Test
    void testServiceStartNullBufferThrows() {
        KinesisService kinesisService = createObjectUnderTest();
        assertThrows(IllegalStateException.class, () -> kinesisService.start(null));

        verify(scheduler, times(0)).run();
    }

    @Test
    void testServiceStartNullStreams() {
        when(kinesisSourceConfig.getStreams()).thenReturn(null);

        KinesisService kinesisService = createObjectUnderTest();
        assertThrows(IllegalStateException.class, () -> kinesisService.start(buffer));

        verify(scheduler, times(0)).run();
    }

    @Test
    void testServiceStartEmptyStreams() {
        when(kinesisSourceConfig.getStreams()).thenReturn(new ArrayList<>());

        KinesisService kinesisService = createObjectUnderTest();
        assertThrows(IllegalStateException.class, () -> kinesisService.start(buffer));

        verify(scheduler, times(0)).run();
    }

    @Test
    public void testShutdownGraceful() {
        KinesisService kinesisService = createObjectUnderTest();
        kinesisService.setScheduler(scheduler);
        kinesisService.shutDown();

        verify(scheduler).startGracefulShutdown();
        verify(scheduler, times(0)).shutdown();
    }

    @Test
    public void testShutdownGracefulThrowInterruptedException() {
        KinesisService kinesisService = createObjectUnderTest();

        when(scheduler.startGracefulShutdown()).thenReturn(CompletableFuture.failedFuture(new InterruptedException()));
        kinesisService.setScheduler(scheduler);
        assertDoesNotThrow(kinesisService::shutDown);

        verify(scheduler).startGracefulShutdown();
        verify(scheduler, times(1)).shutdown();
    }

    @Test
    public void testShutdownGracefulThrowTimeoutException() {
        KinesisService kinesisService = createObjectUnderTest();
        kinesisService.setScheduler(scheduler);
        when(scheduler.startGracefulShutdown()).thenReturn(CompletableFuture.failedFuture(new TimeoutException()));
        assertDoesNotThrow(kinesisService::shutDown);

        verify(scheduler).startGracefulShutdown();
        verify(scheduler, times(1)).shutdown();
    }

    @Test
    public void testShutdownGracefulThrowExecutionException() {
        KinesisService kinesisService = createObjectUnderTest();
        kinesisService.setScheduler(scheduler);
        when(scheduler.startGracefulShutdown()).thenReturn(CompletableFuture.failedFuture(new ExecutionException(new Throwable())));
        assertDoesNotThrow(kinesisService::shutDown);

        verify(scheduler).startGracefulShutdown();
        verify(scheduler, times(1)).shutdown();
    }

    @Test
    public void testShutdownExecutorServiceInterruptedException() {
        when(scheduler.startGracefulShutdown()).thenReturn(CompletableFuture.failedFuture(new InterruptedException()));

        KinesisService kinesisService = createObjectUnderTest();
        kinesisService.setScheduler(scheduler);
        kinesisService.shutDown();

        verify(scheduler).startGracefulShutdown();
        verify(scheduler).shutdown();
    }

}
