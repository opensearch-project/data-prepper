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

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonInputCodec;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonInputConfig;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfig;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfigSupplier;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseCoordinationTableConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.ConsumerStrategy;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.ingester.KinesisIngester;
import org.opensearch.dataprepper.plugins.kinesis.source.util.TestIDGenerator;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_RECORD_PROCESSED;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_RECORD_PROCESSING_ERRORS;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_CHECKPOINT_FAILURES;
import static org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisRecordProcessor.KINESIS_STREAM_TAG_KEY;

public class KinesisSourceIT {
    private static final String AWS_REGION = "tests.kinesis.source.aws.region";
    private static final String STREAM_NAME_PREFIX = "Stream1";
    private static final String PIPELINE_NAME = "pipeline-id";
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMillis(0);
    private static final String codec_plugin_name = "ndjson";
    private static final String LEASE_TABLE_PREFIX = "kinesis-lease-table";
    private static final int NUMBER_OF_RECORDS_TO_ACCUMULATE = 10;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private KinesisClientFactory kinesisClientFactory;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private KinesisSourceConfig kinesisSourceConfig;

    @Mock
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @Mock
    private KinesisStreamConfig kinesisStreamConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier;

    @Mock
    private KinesisLeaseConfig kinesisLeaseConfig;

    @Mock
    private WorkerIdentifierGenerator workerIdentifierGenerator;

    @Mock
    private KinesisLeaseCoordinationTableConfig kinesisLeaseCoordinationTableConfig;

    @Mock
    private InputCodec codec;

    @Mock
    private Counter acknowledgementSetSuccesses;

    @Mock
    private Counter acknowledgementSetFailures;

    @Mock
    private Counter recordsProcessed;

    @Mock
    private Counter recordProcessingErrors;

    @Mock
    private Counter checkpointFailures;

    private KinesisClient kinesisClient;

    private DynamoDbClient dynamoDbClient;

    private KinesisIngester kinesisIngester;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        when(awsAuthenticationConfig.getAwsRegion()).thenReturn(Region.of(System.getProperty(AWS_REGION)));
        when(awsAuthenticationConfig.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsExternalId()).thenReturn(UUID.randomUUID().toString());
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
        when(kinesisSourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);

        String testId = TestIDGenerator.generateRandomTestID();
        String streamName = STREAM_NAME_PREFIX + "_" + testId;
        String leaseTableName = LEASE_TABLE_PREFIX + "_" + testId;

        when(kinesisStreamConfig.getName()).thenReturn(streamName);
        when(kinesisStreamConfig.getCheckPointInterval()).thenReturn(CHECKPOINT_INTERVAL);
        when(kinesisStreamConfig.getInitialPosition()).thenReturn(InitialPositionInStream.TRIM_HORIZON);
        when(kinesisSourceConfig.getConsumerStrategy()).thenReturn(ConsumerStrategy.ENHANCED_FAN_OUT);
        when(kinesisSourceConfig.getStreams()).thenReturn(List.of(kinesisStreamConfig));
        when(kinesisLeaseConfig.getLeaseCoordinationTable()).thenReturn(kinesisLeaseCoordinationTableConfig);
        when(kinesisLeaseCoordinationTableConfig.getTableName()).thenReturn(leaseTableName);
        when(kinesisLeaseCoordinationTableConfig.getRegion()).thenReturn(System.getProperty(AWS_REGION));
        when(kinesisLeaseCoordinationTableConfig.getAwsRegion()).thenReturn(Region.of(System.getProperty(AWS_REGION)));
        when(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig()).thenReturn(Optional.ofNullable(kinesisLeaseConfig));

        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn(codec_plugin_name);
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        when(kinesisSourceConfig.getNumberOfRecordsToAccumulate()).thenReturn(NUMBER_OF_RECORDS_TO_ACCUMULATE);
        when(kinesisSourceConfig.getBufferTimeout()).thenReturn(Duration.ofMillis(1));

        kinesisClientFactory = mock(KinesisClientFactory.class);
        when(kinesisClientFactory.buildDynamoDBClient(kinesisLeaseCoordinationTableConfig.getAwsRegion())).thenReturn(DynamoDbAsyncClient.builder()
                .region(Region.of(System.getProperty(AWS_REGION)))
                .build());
        when(kinesisClientFactory.buildKinesisAsyncClient(awsAuthenticationConfig.getAwsRegion())).thenReturn(KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder()
                        .region(Region.of(System.getProperty(AWS_REGION)))
        ));
        when(kinesisClientFactory.buildCloudWatchAsyncClient(kinesisLeaseCoordinationTableConfig.getAwsRegion())).thenReturn(CloudWatchAsyncClient.builder()
                .region(Region.of(System.getProperty(AWS_REGION)))
                .build());

        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);

        final String workerId = "worker-identifier-" + testId;
        when(workerIdentifierGenerator.generate()).thenReturn(workerId);

        when(pluginMetrics.counterWithTags(ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamName))
                .thenReturn(acknowledgementSetSuccesses);

        when(pluginMetrics.counterWithTags(ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamName))
                .thenReturn(acknowledgementSetFailures);

        when(pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSED, KINESIS_STREAM_TAG_KEY, streamName))
                .thenReturn(recordsProcessed);

        when(pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSING_ERRORS, KINESIS_STREAM_TAG_KEY, streamName))
                .thenReturn(recordProcessingErrors);

        when(pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamName))
                .thenReturn(checkpointFailures);

        kinesisClient = KinesisClient.builder().region(Region.of(System.getProperty(AWS_REGION))).build();
        dynamoDbClient = DynamoDbClient.builder().region(Region.of(System.getProperty(AWS_REGION))).build();
        kinesisIngester = new KinesisIngester(kinesisClient, streamName, dynamoDbClient, leaseTableName);

        kinesisIngester.createStream();
        kinesisIngester.createLeaseTable();
    }

    @AfterEach
    void cleanup() {
        kinesisIngester.deleteLeaseTable();
        kinesisIngester.deleteStream();
    }

    @Test
    public void testKinesisService() throws Exception {
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any()))
                .thenReturn(new NdjsonInputCodec(new NdjsonInputConfig(), TestEventFactory.getTestEventFactory()));

        final List<Record<?>> actualRecordsWritten = new ArrayList<>();
        doAnswer(a -> actualRecordsWritten.addAll(a.getArgument(0, Collection.class)))
                .when(buffer).writeAll(anyCollection(), anyInt());

        // Send data to stream
        final List<String> logStream = new ArrayList<>();
        int numberOfRecords = 20;
        for (int i = 1; i <= numberOfRecords; i++) {
            logStream.add("input record" + i);
        }
        kinesisIngester.ingest(logStream);

        KinesisService kinesisService = new KinesisService(
            kinesisSourceConfig,
            kinesisClientFactory,
            pluginMetrics,
            pluginFactory,
            pipelineDescription,
            acknowledgementSetManager,
            kinesisLeaseConfigSupplier,
            workerIdentifierGenerator
        );

        kinesisService.start(buffer);

        await().atMost(Duration.ofSeconds(150)).untilAsserted(
                () -> {
                    verify(buffer, atLeastOnce()).writeAll(anyCollection(), anyInt());
                    assertThat(actualRecordsWritten, notNullValue());
                    assertThat(actualRecordsWritten.size(), equalTo(numberOfRecords));
                }
        );

        kinesisService.shutDown();
    }
}