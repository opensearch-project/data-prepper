package org.opensearch.dataprepper.plugins.source.dynamodb.leader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.DynamoDBSourceConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamStartPosition;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ContinuousBackupsDescription;
import software.amazon.awssdk.services.dynamodb.model.ContinuousBackupsStatus;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsResponse;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private DynamoDbClient dynamoDbClient;

//    @Mock
//    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private ShardManager shardManager;

    @Mock
    private DynamoDBSourceConfig sourceConfig;

    @Mock
    private TableConfig tableConfig;

    @Mock
    private ExportConfig exportConfig;

    @Mock
    private StreamConfig streamConfig;

    private LeaderPartition leaderPartition;

    private LeaderScheduler leaderScheduler;


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

        // Mock configurations

        lenient().when(streamConfig.getStartPosition()).thenReturn(StreamStartPosition.LATEST);
        lenient().when(exportConfig.getS3Bucket()).thenReturn(bucketName);
        lenient().when(exportConfig.getS3Prefix()).thenReturn(prefix);
        lenient().when(exportConfig.getS3SseKmsKeyId()).thenReturn(null);

        lenient().when(tableConfig.getTableArn()).thenReturn(tableArn);
        lenient().when(tableConfig.getExportConfig()).thenReturn(exportConfig);
        lenient().when(tableConfig.getStreamConfig()).thenReturn(streamConfig);

        // Mock Shard manager
        List<Shard> shardList = new ArrayList<>();
        shardList.add(buildShard("shardId-006", "shardId-003", true));
        shardList.add(buildShard("shardId-005", "shardId-003", true));
        shardList.add(buildShard("shardId-004", "shardId-002", false));
        shardList.add(buildShard("shardId-003", "shardId-001", false));
        shardList.add(buildShard("shardId-002", null, false));
        shardList.add(buildShard("shardId-001", null, false));
        lenient().when(shardManager.runDiscovery(anyString())).thenReturn(shardList);

        List<EnhancedSourcePartition> completedPartitions = new ArrayList<>();
        completedPartitions.add(new StreamPartition(streamArn, "shardId-002", Optional.of(new StreamProgressState())));
        completedPartitions.add(new StreamPartition(streamArn, "shardId-003", Optional.of(new StreamProgressState())));

        lenient().when(coordinator.queryCompletedPartitions(eq(StreamPartition.PARTITION_TYPE), any(Instant.class))).thenReturn(completedPartitions);
        lenient().when(shardManager.findChildShardIds(anyString(), eq("shardId-002"))).thenReturn(List.of("shardId-004"));
        lenient().when(shardManager.findChildShardIds(anyString(), eq("shardId-003"))).thenReturn(List.of("shardId-005", "shardId-006"));

        // Mock SDK Calls
        DescribeTableResponse defaultDescribeTableResponse = generateDescribeTableResponse(StreamViewType.NEW_IMAGE);
        lenient().when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(defaultDescribeTableResponse);

        DescribeContinuousBackupsResponse defaultDescribePITRResponse = generatePITRResponse(true);
        lenient().when(dynamoDbClient.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class))).thenReturn(defaultDescribePITRResponse);

        lenient().when(dynamoDbClient.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class))).thenReturn(defaultDescribePITRResponse);

    }


    @Test
    void test_non_leader_run() throws InterruptedException {
        leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, List.of(tableConfig));
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> leaderScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();

        verifyNoInteractions(shardManager);
        verifyNoInteractions(dynamoDbClient);

    }

    @Test
    void test_shardDiscovery_should_create_children_partitions() throws InterruptedException {
        leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, List.of(tableConfig));
        leaderPartition = new LeaderPartition();
        leaderPartition.getProgressState().get().setInitialized(true);
        leaderPartition.getProgressState().get().setStreamArns(List.of(streamArn));
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> leaderScheduler.run());

        Thread.sleep(100);
        executorService.shutdownNow();
        // Already init
        verifyNoInteractions(dynamoDbClient);

        // Should check the completed partitions
        verify(coordinator).queryCompletedPartitions(eq(StreamPartition.PARTITION_TYPE), any(Instant.class));

        // Should create 3 stream partitions for child shards found
        verify(coordinator, times(3)).createPartition(any(EnhancedSourcePartition.class));

        verify(coordinator).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));

    }

    @Test
    void test_should_init() throws InterruptedException {

        leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, List.of(tableConfig));
        leaderPartition = new LeaderPartition();
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));
        given(coordinator.queryCompletedPartitions(eq(StreamPartition.PARTITION_TYPE), any(Instant.class))).willReturn(Collections.emptyList());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> leaderScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();


        // Should call describe table to get basic table info
        ArgumentCaptor<DescribeTableRequest> describeTableRequestArgumentCaptor = ArgumentCaptor.forClass(DescribeTableRequest.class);
        verify(dynamoDbClient).describeTable(describeTableRequestArgumentCaptor.capture());
        DescribeTableRequest actualDescribeTableRequest = describeTableRequestArgumentCaptor.getValue();
        assertThat(actualDescribeTableRequest.tableName(), equalTo(tableArn));
        // Should check PITR enabled or not
        verify(dynamoDbClient).describeContinuousBackups(any(DescribeContinuousBackupsRequest.class));
        // Acquire the init partition
        verify(coordinator).acquireAvailablePartition(eq(LeaderPartition.PARTITION_TYPE));

        // Should create 1 export partition + 2 stream partitions (2 roots) + 1 global table state
        verify(coordinator, times(4)).createPartition(any(EnhancedSourcePartition.class));

        assertThat(leaderPartition.getProgressState().get().isInitialized(), equalTo(true));
    }


    @Test
    void test_PITR_not_enabled_init_should_failed() throws InterruptedException {
        leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, List.of(tableConfig));
        leaderPartition = new LeaderPartition();
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));

        // If PITR is not enabled
        DescribeContinuousBackupsResponse response = generatePITRResponse(false);
        when(dynamoDbClient.describeContinuousBackups(any(DescribeContinuousBackupsRequest.class))).thenReturn(response);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> leaderScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();

        // Should call describe table to get basic table info
        ArgumentCaptor<DescribeTableRequest> describeTableRequestArgumentCaptor = ArgumentCaptor.forClass(DescribeTableRequest.class);
        verify(dynamoDbClient).describeTable(describeTableRequestArgumentCaptor.capture());
        DescribeTableRequest actualDescribeTableRequest = describeTableRequestArgumentCaptor.getValue();
        assertThat(actualDescribeTableRequest.tableName(), equalTo(tableArn));

        // Should check PITR enabled or not
        verify(dynamoDbClient).describeContinuousBackups(any(DescribeContinuousBackupsRequest.class));

        assertThat(leaderPartition.getProgressState().get().isInitialized(), equalTo(false));

    }

    @Test
    void test_streaming_not_enabled_init_should_failed() throws InterruptedException {

        leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, List.of(tableConfig));
        leaderPartition = new LeaderPartition();
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));

        // If streaming is not enabled
        DescribeTableResponse defaultDescribeTableResponse = generateDescribeTableResponse(null);
        lenient().when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(defaultDescribeTableResponse);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> leaderScheduler.run());
        Thread.sleep(100);
        executorService.shutdownNow();

        // Should call describe table to get basic table info
        ArgumentCaptor<DescribeTableRequest> describeTableRequestArgumentCaptor = ArgumentCaptor.forClass(DescribeTableRequest.class);
        verify(dynamoDbClient).describeTable(describeTableRequestArgumentCaptor.capture());
        DescribeTableRequest actualDescribeTableRequest = describeTableRequestArgumentCaptor.getValue();
        assertThat(actualDescribeTableRequest.tableName(), equalTo(tableArn));

        // Should check PITR enabled or not
        verify(dynamoDbClient).describeContinuousBackups(any(DescribeContinuousBackupsRequest.class));

        assertThat(leaderPartition.getProgressState().get().isInitialized(), equalTo(false));


    }

    @Test
    void run_without_acquiring_leader_partition_does_not_save_null_state() {

        final Duration leastInterval = Duration.ofMillis(200);

        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE))
                .willReturn(Optional.empty());

        leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, List.of(tableConfig), leastInterval);
        leaderPartition = new LeaderPartition();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> leaderScheduler.run());

        await().atMost(leastInterval.multipliedBy(5))
                .untilAsserted(() -> verify(coordinator, atLeast(3))
                        .acquireAvailablePartition(LeaderPartition.PARTITION_TYPE));
        executorService.shutdownNow();

        verify(coordinator, atLeast(3)).acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);
        verify(coordinator, never()).saveProgressStateForPartition(isNull(), any(Duration.class));
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

    private Shard buildShard(String shardId, String parentShardId, boolean isOpen) {
        String endingSequenceNumber = isOpen ? null : UUID.randomUUID().toString();
        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(SequenceNumberRange.builder()
                        .endingSequenceNumber(endingSequenceNumber)
                        .startingSequenceNumber(UUID.randomUUID().toString())
                        .build())
                .build();

    }
}