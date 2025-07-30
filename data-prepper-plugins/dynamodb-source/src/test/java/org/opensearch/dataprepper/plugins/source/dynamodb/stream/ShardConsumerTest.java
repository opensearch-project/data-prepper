/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.DynamoDBSourceAggregateMetrics;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.dynamodb.stream.ShardConsumer.BUFFER_TIMEOUT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.stream.ShardConsumer.DEFAULT_BUFFER_BATCH_SIZE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.stream.ShardConsumer.SHARD_PROGRESS;

@ExtendWith(MockitoExtension.class)
class ShardConsumerTest {
    private static final Random RANDOM = new Random();

    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DynamoDBSourceAggregateMetrics aggregateMetrics;

    @Mock
    private Counter stream5xxErrors;

    @Mock
    private Counter stream4xxErrors;

    @Mock
    private Counter streamApiInvocations;

    @Mock
    private Counter shardProgress;

    @Mock
    private Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer;

    @Mock
    private BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> bufferAccumulator;

    @Mock
    private GlobalState tableInfoGlobalState;

    @Mock
    private Counter testCounter;

    @Mock
    private DistributionSummary testSummary;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private ShardAcknowledgementManager shardAcknowledgementManager;

    private StreamPartition streamPartition;

    private TableInfo tableInfo;

    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String partitionKeyAttrName = "PK";
    private final String sortKeyAttrName = "SK";

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();
    private final String shardIterator = UUID.randomUUID().toString();

    private final Random random = new Random();

    private final int total = random.nextInt(10) + 1;

    @BeforeEach
    void setup() throws Exception {

        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(Instant.now().toEpochMilli());
        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));

        // Mock Global Table Info
        lenient().when(coordinator.getPartition(tableArn)).thenReturn(Optional.of(tableInfoGlobalState));
        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName(partitionKeyAttrName)
                .sortKeyAttributeName(sortKeyAttrName)
                .build();
        tableInfo = new TableInfo(tableArn, metadata);
        lenient().when(tableInfoGlobalState.getProgressState()).thenReturn(Optional.of(metadata.toMap()));

        lenient().when(coordinator.createPartition(any(EnhancedSourcePartition.class))).thenReturn(true);
        lenient().doNothing().when(coordinator).completePartition(any(EnhancedSourcePartition.class));
        lenient().doNothing().when(coordinator).saveProgressStateForPartition(any(EnhancedSourcePartition.class), any(Duration.class));
        lenient().doNothing().when(coordinator).giveUpPartition(any(EnhancedSourcePartition.class));

        lenient().doNothing().when(bufferAccumulator).add(any(org.opensearch.dataprepper.model.record.Record.class));
        lenient().doNothing().when(bufferAccumulator).flush();

        List<Record> records = buildRecords(total);
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(records)
                .nextShardIterator(null)
                .build();
        lenient().when(dynamoDbStreamsClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        given(pluginMetrics.counter(SHARD_PROGRESS)).willReturn(shardProgress);
        given(pluginMetrics.counter("changeEventsProcessed")).willReturn(testCounter);
        given(pluginMetrics.counter("changeEventsProcessingErrors")).willReturn(testCounter);
        given(pluginMetrics.summary(anyString())).willReturn(testSummary);

        lenient().when(aggregateMetrics.getStreamApiInvocations()).thenReturn(streamApiInvocations);


        lenient().when(shardAcknowledgementManager.isExportDone(any(StreamPartition.class))).thenReturn(true);
    }

    @Test
    void test_run_shardConsumer_correctly() throws Exception {
        // Disable the static shouldStop flag to prevent early exit
        try (MockedStatic<ShardConsumer> shardConsumerMockedStatic = mockStatic(ShardConsumer.class, invocation -> {
            if (invocation.getMethod().getName().equals("stopAll")) {
                return null;
            } else if (invocation.getMethod().getName().equals("shouldStop")) {
                return false;
            }
            return invocation.callRealMethod();
        })) {
            ShardConsumer shardConsumer;
            try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
                bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
                shardConsumer = ShardConsumer.builder(dynamoDbStreamsClient, pluginMetrics, aggregateMetrics, buffer, streamConfig)
                        .shardIterator(shardIterator)
                        .shardAcknowledgementManager(shardAcknowledgementManager)
                        .streamPartition(streamPartition)
                        .tableInfo(tableInfo)
                        .startTime(null)
                        .waitForExport(false)
                        .build();
            }

            shardConsumer.run();

            verify(dynamoDbStreamsClient).getRecords(any(GetRecordsRequest.class));
            verify(bufferAccumulator, times(total)).add(any(org.opensearch.dataprepper.model.record.Record.class));
            verify(bufferAccumulator).flush();
            verify(streamApiInvocations).increment();
            verify(shardProgress).increment();
        }
    }

    @Test
    void test_run_shardConsumer_with_acknowledgments_correctly() throws Exception {
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        
        // Mock the shardAcknowledgementManager to return our mock acknowledgementSet
        lenient().when(shardAcknowledgementManager.createAcknowledgmentSet(any(StreamPartition.class), any(String.class), any(Boolean.class)))
                .thenReturn(acknowledgementSet);

        // Disable the static shouldStop flag to prevent early exit
        try (MockedStatic<ShardConsumer> shardConsumerMockedStatic = mockStatic(ShardConsumer.class, invocation -> {
            if (invocation.getMethod().getName().equals("stopAll")) {
                return null;
            } else if (invocation.getMethod().getName().equals("shouldStop")) {
                return false;
            }
            return invocation.callRealMethod();
        })) {
            ShardConsumer shardConsumer;
            try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
                bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
                shardConsumer = ShardConsumer.builder(dynamoDbStreamsClient, pluginMetrics, aggregateMetrics, buffer, streamConfig)
                        .shardIterator(shardIterator)
                        .shardAcknowledgementManager(shardAcknowledgementManager)
                        .streamPartition(streamPartition)
                        .tableInfo(tableInfo)
                        .startTime(null)
                        .waitForExport(false)
                        .build();
            }

            shardConsumer.run();

            verify(dynamoDbStreamsClient).getRecords(any(GetRecordsRequest.class));
            verify(bufferAccumulator, times(total)).add(any(org.opensearch.dataprepper.model.record.Record.class));
            verify(bufferAccumulator).flush();
            verify(streamApiInvocations).increment();
            verify(shardProgress).increment();
        }
    }

    @Test
    void test_run_shardConsumer_with_acknowledgments_and_error_cancels_acknowledgment_set() throws Exception {
        when(dynamoDbStreamsClient.getRecords(any(GetRecordsRequest.class))).thenThrow(InternalServerErrorException.class);
        when(aggregateMetrics.getStream5xxErrors()).thenReturn(stream5xxErrors);

        // Disable the static shouldStop flag to prevent early exit
        try (MockedStatic<ShardConsumer> shardConsumerMockedStatic = mockStatic(ShardConsumer.class, invocation -> {
            if (invocation.getMethod().getName().equals("stopAll")) {
                return null;
            }
            return invocation.callRealMethod();
        })) {
            ShardConsumer shardConsumer;
            try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
                bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
                shardConsumer = ShardConsumer.builder(dynamoDbStreamsClient, pluginMetrics, aggregateMetrics, buffer, streamConfig)
                        .shardIterator(shardIterator)
                        .shardAcknowledgementManager(shardAcknowledgementManager)
                        .streamPartition(streamPartition)
                        .tableInfo(tableInfo)
                        .startTime(null)
                        .waitForExport(false)
                        .build();
            }

            assertThrows(RuntimeException.class, shardConsumer::run);
            
            verify(stream5xxErrors).increment();
            verify(streamApiInvocations).increment();
        }
    }

    @Test
    void test_run_shardConsumer_catches_5xx_exception_and_increments_metric() {
        // First set up the mocks for the exception case
        when(dynamoDbStreamsClient.getRecords(any(GetRecordsRequest.class))).thenThrow(InternalServerErrorException.class);
        when(aggregateMetrics.getStream5xxErrors()).thenReturn(stream5xxErrors);
        
        // Disable the static shouldStop flag to prevent early exit
        try (MockedStatic<ShardConsumer> shardConsumerMockedStatic = mockStatic(ShardConsumer.class, invocation -> {
            if (invocation.getMethod().getName().equals("stopAll")) {
                return null;
            }
            return invocation.callRealMethod();
        })) {
            ShardConsumer shardConsumer;
            try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
                bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
                shardConsumer = ShardConsumer.builder(dynamoDbStreamsClient, pluginMetrics, aggregateMetrics, buffer, streamConfig)
                        .shardIterator(shardIterator)
                        .shardAcknowledgementManager(shardAcknowledgementManager)
                        .streamPartition(streamPartition)
                        .tableInfo(tableInfo)
                        .startTime(null)
                        .waitForExport(false)
                        .build();
            }

            assertThrows(RuntimeException.class, shardConsumer::run);

            verify(stream5xxErrors).increment();
            verify(streamApiInvocations).increment();
        }
    }

    @Test
    void test_run_shardConsumer_catches_4xx_exception_and_increments_metric() {
        // First set up the mocks for the exception case
        when(dynamoDbStreamsClient.getRecords(any(GetRecordsRequest.class))).thenThrow(DynamoDbException.class);
        when(aggregateMetrics.getStream4xxErrors()).thenReturn(stream4xxErrors);
        
        // Disable the static shouldStop flag to prevent early exit
        try (MockedStatic<ShardConsumer> shardConsumerMockedStatic = mockStatic(ShardConsumer.class, invocation -> {
            if (invocation.getMethod().getName().equals("stopAll")) {
                return null;
            }
            return invocation.callRealMethod();
        })) {
            ShardConsumer shardConsumer;
            try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
                bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
                shardConsumer = ShardConsumer.builder(dynamoDbStreamsClient, pluginMetrics, aggregateMetrics, buffer, streamConfig)
                        .shardIterator(shardIterator)
                        .shardAcknowledgementManager(shardAcknowledgementManager)
                        .streamPartition(streamPartition)
                        .tableInfo(tableInfo)
                        .startTime(null)
                        .waitForExport(false)
                        .build();
            }

            assertThrows(RuntimeException.class, shardConsumer::run);

            verify(stream4xxErrors).increment();
            verify(streamApiInvocations).increment();
        }
    }

    @Test
    void test_run_shardConsumer_calls_startUpdatingOwnershipForShard() throws Exception {
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
            ShardConsumer shardConsumer = ShardConsumer.builder(dynamoDbStreamsClient, pluginMetrics, aggregateMetrics, buffer, streamConfig)
                    .shardIterator(null)
                    .shardAcknowledgementManager(shardAcknowledgementManager)
                    .streamPartition(streamPartition)
                    .tableInfo(tableInfo)
                    .startTime(null)
                    .waitForExport(false)
                    .build();

            shardConsumer.run();
        }
        // Verify that startUpdatingOwnershipForShard is called
        verify(shardAcknowledgementManager).startUpdatingOwnershipForShard(streamPartition);
    }

    private List<Record> buildRecords(int count) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, AttributeValue> data = Map.of(
                    partitionKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                    sortKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build());

            StreamRecord streamRecord = StreamRecord.builder()
                    .sizeBytes(RANDOM.nextLong())
                    .newImage(data)
                    .sequenceNumber(UUID.randomUUID().toString())
                    .approximateCreationDateTime(Instant.now())
                    .build();
            Record record = Record.builder()
                    .dynamodb(streamRecord)
                    .eventName(OperationType.INSERT)
                    .build();
            records.add(record);
        }

        return records;
    }
}