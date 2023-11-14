package org.opensearch.dataprepper.plugins.source.dynamodb.leader;

import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamStartPosition;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);

    /**
     * Default duration to extend the timeout of lease
     */
    private static final int DEFAULT_EXTEND_LEASE_MINUTES = 3;

    /**
     * Default interval to run lease check and shard discovery
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 60_000;

    private final List<TableConfig> tableConfigs;

    private final EnhancedSourceCoordinator coordinator;

    private final DynamoDbClient dynamoDbClient;

    private final ShardManager shardManager;

    private LeaderPartition leaderPartition;

    private List<String> streamArns;

    public LeaderScheduler(EnhancedSourceCoordinator coordinator, DynamoDbClient dynamoDbClient, ShardManager shardManager, List<TableConfig> tableConfigs) {
        this.tableConfigs = tableConfigs;
        this.coordinator = coordinator;
        this.dynamoDbClient = dynamoDbClient;
        this.shardManager = shardManager;
    }

    @Override
    public void run() {
        LOG.debug("Starting Leader Scheduler for initialization and shard discovery");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Try to acquire the lease if not owned.
                if (leaderPartition == null) {
                    final Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);
                    if (sourcePartition.isPresent()) {
                        LOG.info("Running as a LEADER node");
                        leaderPartition = (LeaderPartition) sourcePartition.get();
                    }
                }
                // Once owned, run Normal LEADER node process.
                // May want to quit this scheduler if streaming is not required
                if (leaderPartition != null) {
                    LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
                    if (!leaderProgressState.isInitialized()) {
                        LOG.debug("The service is not been initialized");
                        init();
                    } else {
                        // The initialization process will populate that value, otherwise, get from state
                        if (streamArns == null) {
                            streamArns = leaderProgressState.getStreamArns();
                        }
                    }

                    if (streamArns != null && !streamArns.isEmpty()) {
                        // Step 1: Run shard discovery
                        streamArns.forEach(streamArn -> {
                            shardManager.runDiscovery(streamArn);
                        });

                        // Step 2: Search all completed shards in the last 1 day (maximum time)
                        List<EnhancedSourcePartition> sourcePartitions = coordinator.queryCompletedPartitions(
                                StreamPartition.PARTITION_TYPE,
                                Instant.now().minus(Duration.ofDays(1))
                        );

                        // Step 3: Find and create children partitions.
                        compareAndCreateChildrenPartitions(sourcePartitions);

                        // Extend the timeout
                        // will always be a leader until shutdown
                        coordinator.saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));
                    }

                }

            } catch (Exception e) {
                LOG.error("Exception occurred in primary scheduling loop", e);
            } finally {
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("InterruptedException occurred");
                    break;
                }
            }
        }
        // Should Stop
        LOG.warn("Quitting Leader Scheduler");
        if (leaderPartition != null) {
            coordinator.giveUpPartition(leaderPartition);
        }
    }

    private void init() {
        LOG.info("Try to initialize DynamoDB service");
        List<TableInfo> tableInfos = tableConfigs.stream().map(this::getTableInfo).collect(Collectors.toList());
        streamArns = new ArrayList<>();

        tableInfos.forEach(tableInfo -> {
            // Create a Global state in the coordination table for the configuration.
            // Global State here is designed to be able to read whenever needed
            // So that the jobs can refer to the configuration.
            coordinator.createPartition(new GlobalState(tableInfo.getTableArn(), Optional.of(tableInfo.getMetadata().toMap())));

            Instant startTime = Instant.now();
            if (tableInfo.getMetadata().isExportRequired()) {
                createExportPartition(
                        tableInfo.getTableArn(),
                        startTime,
                        tableInfo.getMetadata().getExportBucket(),
                        tableInfo.getMetadata().getExportPrefix(),
                        tableInfo.getMetadata().getExportKmsKeyId());
            }

            if (tableInfo.getMetadata().isStreamRequired()) {
                // TODO: Revisit the use of start position.
                // The behaviour is same for all cases regardless the configuration provided.
                // Only process event from current date time but still traverse the shards from the beginning.
                List<Shard> shards = shardManager.runDiscovery(tableInfo.getMetadata().getStreamArn());
                List<String> childIds = shards.stream().map(shard -> shard.shardId()).collect(Collectors.toList());
                // Create for root shards.

                List<Shard> rootShards = shards.stream()
                        .filter(shard -> shard.parentShardId() == null || !childIds.contains(shard.parentShardId()))
                        .collect(Collectors.toList());
                LOG.info("Found {} root shards in total", rootShards.size());
                rootShards.forEach(shard -> {
                    createRootStreamPartition(tableInfo.getMetadata().getStreamArn(), shard, startTime, tableInfo.getMetadata().isExportRequired());
                });
                streamArns.add(tableInfo.getMetadata().getStreamArn());
            }

        });

        LOG.debug("Update initialization state");
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setStreamArns(streamArns);
        leaderProgressState.setInitialized(true);
        coordinator.saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));
    }


    /**
     * Compare and find all child shards.
     * Then try to create a stream partition for each.
     */
    private void compareAndCreateChildrenPartitions(List<EnhancedSourcePartition> sourcePartitions) {

        if (sourcePartitions == null || sourcePartitions.isEmpty()) {
            return;
        }
        long startTime = System.currentTimeMillis();
        // Get the list of completed shard Ids.
        List<String> completedShardIds = sourcePartitions.stream()
                .map(sourcePartition -> ((StreamPartition) sourcePartition).getShardId())
                .collect(Collectors.toList());
        // Try to Create a stream partition for each child shards that have been found and not completed yet.
        // If a shard is already created, it won't be created again.
        sourcePartitions.forEach(sourcePartition -> {
            StreamPartition streamPartition = (StreamPartition) sourcePartition;
            List<String> childShardIds = shardManager.findChildShardIds(streamPartition.getStreamArn(), streamPartition.getShardId());
            if (childShardIds != null && !childShardIds.isEmpty()) {
                childShardIds.forEach(
                        shardId -> {
                            if (!completedShardIds.contains(shardId)) {
                                createChildStreamPartition(streamPartition, shardId);
                            }
                        }
                );
            }
        });
        long endTime = System.currentTimeMillis();
        LOG.info("Compare and create children partitions took {} milliseconds", endTime - startTime);
    }


    /**
     * Conduct Metadata info for table and also perform validation on configuration.
     * Once created, the info should not be changed.
     */
    private TableInfo getTableInfo(TableConfig tableConfig) {
        String tableName = TableUtil.getTableNameFromArn(tableConfig.getTableArn());
        DescribeTableResponse describeTableResult;
        try {
            // Need to call describe table to get the Key schema for table
            // The key schema will be used when adding the metadata to event.
            DescribeTableRequest req = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            describeTableResult = dynamoDbClient.describeTable(req);
        } catch (Exception e) {
            LOG.error("Unable to call DescribeTableRequest to get information for table {} due to {}", tableName, e.getMessage());
            throw new RuntimeException("Unable to get table information for " + tableName + ". Please make sure the permission is properly set");
        }

        Map<String, String> keys = describeTableResult.table().keySchema().stream().collect(Collectors.toMap(
                e -> e.keyTypeAsString(), e -> e.attributeName()
        ));
        // Validate if PITR is turn on or not for exports.
        if (tableConfig.getExportConfig() != null) {
            String status = getContinuousBackupsStatus(tableName);
            LOG.debug("The PITR status for table " + tableName + " is " + status);
            if (!"ENABLED".equals(status)) {
                String errorMessage = "Point-in-time recovery (PITR) needs to be enabled for exporting data from table " + tableConfig.getTableArn();
                LOG.error(errorMessage);
                throw new InvalidPluginConfigurationException(errorMessage);
            }
        }

        StreamStartPosition streamStartPosition = null;

        if (tableConfig.getStreamConfig() != null) {
            // Validate if DynamoDB Stream is turn on or not
            if (describeTableResult.table().streamSpecification() == null) {
                String errorMessage = "Steam is not enabled for table " + tableConfig.getTableArn();
                LOG.error(errorMessage);
                throw new InvalidPluginConfigurationException(errorMessage);
            }
            // Validate view type of DynamoDB stream
            String viewType = describeTableResult.table().streamSpecification().streamViewTypeAsString();
            LOG.debug("The stream view type for table " + tableName + " is " + viewType);
            List<String> supportedType = List.of("NEW_IMAGE", "NEW_AND_OLD_IMAGES");
            if (!supportedType.contains(viewType)) {
                String errorMessage = "Stream " + tableConfig.getTableArn() + " is enabled with " + viewType + ". Supported types are " + supportedType;
                LOG.error(errorMessage);
                throw new InvalidPluginConfigurationException(errorMessage);
            }
            streamStartPosition = tableConfig.getStreamConfig().getStartPosition();
        }

        // Conduct metadata info
        // May consider to remove export bucket and prefix
        TableMetadata metadata = TableMetadata.builder()
                .partitionKeyAttributeName(keys.get("HASH"))
                .sortKeyAttributeName(keys.get("RANGE"))
                .streamArn(describeTableResult.table().latestStreamArn())
                .streamRequired(tableConfig.getStreamConfig() != null)
                .exportRequired(tableConfig.getExportConfig() != null)
                .streamStartPosition(streamStartPosition) // Will be ignored
                .exportBucket(tableConfig.getExportConfig() == null ? null : tableConfig.getExportConfig().getS3Bucket())
                .exportPrefix(tableConfig.getExportConfig() == null ? null : tableConfig.getExportConfig().getS3Prefix())
                .exportKmsKeyId(tableConfig.getExportConfig() == null ? null : tableConfig.getExportConfig().getS3SseKmsKeyId())
                .build();
        return new TableInfo(tableConfig.getTableArn(), metadata);
    }

    private String getContinuousBackupsStatus(String tableName) {
        // Validate Point in time recovery is enabled or not
        try {
            DescribeContinuousBackupsRequest req = DescribeContinuousBackupsRequest.builder()
                    .tableName(tableName)
                    .build();
            DescribeContinuousBackupsResponse resp = dynamoDbClient.describeContinuousBackups(req);
            return resp.continuousBackupsDescription().pointInTimeRecoveryDescription().pointInTimeRecoveryStatus().toString();
        } catch (Exception e) {
            LOG.error("Unable to call describeContinuousBackupsRequest for table {} due to {}", tableName, e.getMessage());
            throw new RuntimeException("Unable to check if point in time recovery is enabled or not for " + tableName + ". Please make sure the permission is properly set");
        }
    }


    /**
     * Create a partition for a stream job in the coordination table.
     *
     * @param streamArn  Stream Arn
     * @param shard      A {@link Shard}
     * @param exportTime the start time for change events, any change events with creation datetime before this should be ignored.
     */
    private void createRootStreamPartition(String streamArn, Shard shard, Instant exportTime, boolean waitForExport) {
        StreamProgressState streamProgressState = new StreamProgressState();
        streamProgressState.setWaitForExport(waitForExport);
        streamProgressState.setStartTime(exportTime.toEpochMilli());
        streamProgressState.setEndingSequenceNumber(shard.sequenceNumberRange().endingSequenceNumber());
        coordinator.createPartition(new StreamPartition(streamArn, shard.shardId(), Optional.of(streamProgressState)));
    }

    /**
     * Create a stream partition for a child shard ID. Some of the information are getting from parent.
     */
    private void createChildStreamPartition(StreamPartition streamPartition, String childShardId) {
        StreamProgressState parentStreamProgressState = streamPartition.getProgressState().get();
        StreamProgressState streamProgressState = new StreamProgressState();
        streamProgressState.setStartTime(parentStreamProgressState.getStartTime());
        streamProgressState.setEndingSequenceNumber(shardManager.getEndingSequenceNumber(childShardId));
        streamProgressState.setWaitForExport(parentStreamProgressState.shouldWaitForExport());
        StreamPartition partition = new StreamPartition(streamPartition.getStreamArn(), childShardId, Optional.of(streamProgressState));
        coordinator.createPartition(partition);
    }

    /**
     * Create a partition for an export job in the coordination table. The bucket and prefix will be stored in the progress state.
     * This is to support that different tables can use different destinations.
     *
     * @param tableArn   Table Arn
     * @param exportTime Export Time
     * @param bucket     Export bucket
     * @param prefix     Export Prefix
     * @param kmsKeyId   Export SSE KMS Key ID.
     */
    private void createExportPartition(String tableArn, Instant exportTime, String bucket, String prefix, String kmsKeyId) {
        ExportProgressState exportProgressState = new ExportProgressState();
        exportProgressState.setBucket(bucket);
        exportProgressState.setPrefix(prefix);
        exportProgressState.setExportTime(exportTime.toString()); // information purpose
        exportProgressState.setKmsKeyId(kmsKeyId);
        ExportPartition exportPartition = new ExportPartition(tableArn, exportTime, Optional.of(exportProgressState));
        coordinator.createPartition(exportPartition);
    }

}
