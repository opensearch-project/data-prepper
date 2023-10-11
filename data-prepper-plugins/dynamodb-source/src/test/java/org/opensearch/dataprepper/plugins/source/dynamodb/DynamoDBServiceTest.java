/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.InitPartition;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ContinuousBackupsDescription;
import software.amazon.awssdk.services.dynamodb.model.ContinuousBackupsStatus;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoveryDescription;
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoveryStatus;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DynamoDBServiceTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private ClientFactory clientFactory;

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private S3Client s3Client;

    @Mock
    private DynamoDBSourceConfig sourceConfig;

    @Mock
    private TableConfig tableConfig;

    @Mock
    private ExportConfig exportConfig;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private InitPartition initPartition;

    @Mock
    private Buffer<Record<Event>> buffer;

    private DynamoDBService dynamoDBService;

    private Collection<KeySchemaElement> keySchema;

    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String partitionKeyAttrName = "PK";
    private final String sortKeyAttrName = "SK";
    private final String bucketName = UUID.randomUUID().toString();
    private final String prefix = UUID.randomUUID().toString();

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();

    private final long exportTimeMills = 1695021857760L;
    private final Instant exportTime = Instant.ofEpochMilli(exportTimeMills);


    @BeforeEach
    void setup() {

        KeySchemaElement pk = KeySchemaElement.builder()
                .attributeName(partitionKeyAttrName)
                .keyType(KeyType.HASH)
                .build();
        KeySchemaElement sk = KeySchemaElement.builder()
                .attributeName(sortKeyAttrName)
                .keyType(KeyType.RANGE)
                .build();

        keySchema = List.of(pk, sk);

        // Mock Client Factory
        lenient().when(clientFactory.buildS3Client()).thenReturn(s3Client);
        lenient().when(clientFactory.buildDynamoDBClient()).thenReturn(dynamoDbClient);
        lenient().when(clientFactory.buildDynamoDbStreamClient()).thenReturn(dynamoDbStreamsClient);
        // Mock configurations
        lenient().when(exportConfig.getS3Bucket()).thenReturn(bucketName);
        lenient().when(exportConfig.getS3Prefix()).thenReturn(prefix);
        lenient().when(streamConfig.getStartPosition()).thenReturn("LATEST");
        lenient().when(tableConfig.getTableArn()).thenReturn(tableArn);
        lenient().when(tableConfig.getExportConfig()).thenReturn(exportConfig);
        lenient().when(tableConfig.getStreamConfig()).thenReturn(streamConfig);
        lenient().when(sourceConfig.getTableConfigs()).thenReturn(List.of(tableConfig));

        // Mock SDK Calls
        lenient().when(dynamoDbStreamsClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(generateDescribeStreamResponse());

        DescribeTableResponse defaultDescribeTableResponse = generateDescribeTableResponse(StreamViewType.NEW_IMAGE);
        lenient().when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(defaultDescribeTableResponse);

        DescribeContinuousBackupsResponse defaultDescribePITRResponse = generatePITRResponse(true);
        lenient().when(dynamoDbClient.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class))).thenReturn(defaultDescribePITRResponse);

    }

    private DynamoDBService createObjectUnderTest() {
        DynamoDBService objectUnderTest = new DynamoDBService(coordinator, clientFactory, sourceConfig, pluginMetrics);
        return objectUnderTest;
    }

    @Test
    void test_normal_start() {
        dynamoDBService = createObjectUnderTest();
        assertThat(dynamoDBService, notNullValue());
        dynamoDBService.start(buffer);

    }


    @Test
    void test_normal_shutdown() {
        dynamoDBService = createObjectUnderTest();
        assertThat(dynamoDBService, notNullValue());
        dynamoDBService.shutdown();
    }


    @Test
    void test_should_init() {

        given(coordinator.acquireAvailablePartition(InitPartition.PARTITION_TYPE)).willReturn(Optional.of(initPartition)).willReturn(Optional.empty());

        dynamoDBService = createObjectUnderTest();
        dynamoDBService.init();
        // Should call describe table to get basic table info
        verify(dynamoDbClient).describeTable(any(DescribeTableRequest.class));
        // Should check PITR enabled or not
        verify(dynamoDbClient).describeContinuousBackups(any(DescribeContinuousBackupsRequest.class));
        // Acquire the init partition
        verify(coordinator).acquireAvailablePartition(eq(InitPartition.PARTITION_TYPE));
        // Complete the init partition
        verify(coordinator).completePartition(any(EnhancedSourcePartition.class));

        // Should create 1 export partition + 1 stream partition + 1 global table state
        verify(coordinator, times(3)).createPartition(any(EnhancedSourcePartition.class));
    }

    @Test
    void test_already_init() {
        given(coordinator.acquireAvailablePartition(InitPartition.PARTITION_TYPE)).willReturn(Optional.empty());
        dynamoDBService = createObjectUnderTest();
        dynamoDBService.init();

        verifyNoInteractions(dynamoDbClient);
    }

    @Test
    void test_PITR_not_enabled_should_throw_errors() {

        given(coordinator.acquireAvailablePartition(InitPartition.PARTITION_TYPE)).willReturn(Optional.of(initPartition)).willReturn(Optional.empty());

        // If PITR is not enabled
        DescribeContinuousBackupsResponse response = generatePITRResponse(false);
        when(dynamoDbClient.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class))).thenReturn(response);

        dynamoDBService = createObjectUnderTest();
        assertThrows(
                InvalidPluginConfigurationException.class,
                () -> dynamoDBService.init());

    }

    @Test
    void test_streaming_not_enabled_should_throw_errors() {

        given(coordinator.acquireAvailablePartition(InitPartition.PARTITION_TYPE)).willReturn(Optional.of(initPartition)).willReturn(Optional.empty());

        // If streaming is not enabled
        DescribeTableResponse defaultDescribeTableResponse = generateDescribeTableResponse(null);
        lenient().when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(defaultDescribeTableResponse);

        dynamoDBService = createObjectUnderTest();
        assertThrows(
                InvalidPluginConfigurationException.class,
                () -> dynamoDBService.init());

    }


    /**
     * Helper function to mock DescribeContinuousBackupsResponse
     */
    private DescribeContinuousBackupsResponse generatePITRResponse(boolean enabled) {
        PointInTimeRecoveryDescription pointInTimeRecoveryDescription = PointInTimeRecoveryDescription.builder()
                .pointInTimeRecoveryStatus(enabled ? PointInTimeRecoveryStatus.ENABLED : PointInTimeRecoveryStatus.DISABLED)
                .build();
        ContinuousBackupsDescription continuousBackupsDescription = ContinuousBackupsDescription.builder()
                .continuousBackupsStatus(ContinuousBackupsStatus.ENABLED)
                .pointInTimeRecoveryDescription(pointInTimeRecoveryDescription)
                .build();
        DescribeContinuousBackupsResponse response = DescribeContinuousBackupsResponse.builder()
                .continuousBackupsDescription(continuousBackupsDescription).build();

        return response;
    }

    /**
     * Helper function to mock DescribeStreamResponse
     */
    private DescribeStreamResponse generateDescribeStreamResponse() {
        Shard shard = Shard.builder()
                .shardId(shardId)
                .parentShardId(null)
                .sequenceNumberRange(SequenceNumberRange.builder()
                        .endingSequenceNumber(null)
                        .startingSequenceNumber(UUID.randomUUID().toString())
                        .build())
                .build();

        List<Shard> shardList = new ArrayList<>();
        shardList.add(shard);


        StreamDescription description = StreamDescription.builder()
                .shards(shardList)
                .lastEvaluatedShardId(null)
                .build();
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
                .streamDescription(description)
                .build();
        return describeStreamResponse;
    }

    /**
     * Helper function to mock DescribeTableResponse
     */
    private DescribeTableResponse generateDescribeTableResponse(StreamViewType viewType) {
        StreamSpecification streamSpecification = StreamSpecification.builder()
                .streamEnabled(viewType == null)
                .streamViewType(viewType == null ? StreamViewType.UNKNOWN_TO_SDK_VERSION : viewType)
                .build();

        TableDescription tableDescription = TableDescription.builder()
                .keySchema(keySchema)
                .tableName(tableName)
                .tableArn(tableArn)
                .latestStreamArn(streamArn)
                .streamSpecification(streamSpecification)
                .build();
        DescribeTableResponse response = DescribeTableResponse.builder()
                .table(tableDescription)
                .build();
        return response;
    }
}