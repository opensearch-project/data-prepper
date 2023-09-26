/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.SourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.InitPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileLoaderFactory;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.ManifestFileReader;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.S3ObjectReader;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.dynamodb.stream.ShardConsumerFactory;
import org.opensearch.dataprepper.plugins.source.dynamodb.stream.ShardManager;
import org.opensearch.dataprepper.plugins.source.dynamodb.stream.StreamScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeContinuousBackupsResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DynamoDBService {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBService.class);

    private final List<TableConfig> tableConfigs;

    private final EnhancedSourceCoordinator coordinator;

    private final DynamoDbClient dynamoDbClient;
    //
    private final DynamoDbStreamsClient dynamoDbStreamsClient;

    private final S3Client s3Client;

    private final ShardManager shardManager;

    private final ExecutorService executor;

    private final PluginMetrics pluginMetrics;


    public DynamoDBService(EnhancedSourceCoordinator coordinator, ClientFactory clientFactory, DynamoDBSourceConfig sourceConfig, PluginMetrics pluginMetrics) {
        this.coordinator = coordinator;
        this.pluginMetrics = pluginMetrics;

        // Initialize AWS clients
        dynamoDbClient = clientFactory.buildDynamoDBClient();
        dynamoDbStreamsClient = clientFactory.buildDynamoDbStreamClient();
        s3Client = clientFactory.buildS3Client();

        // A shard manager is responsible to retrieve the shard information from streams.
        shardManager = new ShardManager(dynamoDbStreamsClient);
        tableConfigs = sourceConfig.getTableConfigs();
        executor = Executors.newFixedThreadPool(3);
    }

    /**
     * This service start three long-running threads (scheduler)
     * Each thread is responsible for one type of job.
     * The data will be guaranteed to be sent to {@link Buffer} in order.
     *
     * @param buffer Data Prepper Buffer
     */
    public void start(Buffer<Record<Event>> buffer) {

        LOG.info("Start running DynamoDB service");
        ManifestFileReader manifestFileReader = new ManifestFileReader(new S3ObjectReader(s3Client));
        Runnable exportScheduler = new ExportScheduler(coordinator, dynamoDbClient, manifestFileReader, pluginMetrics);

        DataFileLoaderFactory loaderFactory = new DataFileLoaderFactory(coordinator, s3Client, pluginMetrics, buffer);
        Runnable fileLoaderScheduler = new DataFileScheduler(coordinator, loaderFactory, pluginMetrics);

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, dynamoDbStreamsClient, pluginMetrics, shardManager, buffer);
        Runnable streamScheduler = new StreamScheduler(coordinator, consumerFactory, shardManager);

        // May consider start or shutdown the scheduler on demand
        // Currently, event after the exports are done, the related scheduler will not be shutdown
        // This is because in the future we may support incremental exports.
        executor.submit(exportScheduler);
        executor.submit(fileLoaderScheduler);
        executor.submit(streamScheduler);

    }

    /**
     * Interrupt the running of schedulers.
     * Each scheduler must implement logic for gracefully shutdown.
     */
    public void shutdown() {
        LOG.info("shutdown DynamoDB schedulers");
        executor.shutdownNow();
    }

    /**
     * Perform initialization of the service from pipeline configuration
     * The initialization is currently performed once per pipeline.
     * Hence, the configuration change after first initialization process will be ignored.
     * This is controlled by a lease in the coordination table.
     * Future optimization can be done to accept configuration changes
     */
    public void init() {
        LOG.info("Start initialize DynamoDB service");

        final Optional<SourcePartition> initPartition = coordinator.acquireAvailablePartition(InitPartition.PARTITION_TYPE);
        if (initPartition.isEmpty()) {
            // Already initialized. Do nothing.
            return;
        }

        LOG.info("Start validating table configurations");
        List<TableInfo> tableInfos;
        try {
            tableInfos = tableConfigs.stream().map(this::getTableInfo).collect(Collectors.toList());
        } catch (Exception e) {
            coordinator.giveUpPartition(initPartition.get());
            throw e;
        }

        tableInfos.forEach(tableInfo -> {
            // Create a Global state in the coordination table for the configuration.
            // Global State here is designed to be able to read whenever needed
            // So that the jobs can refer to the configuration.
            coordinator.createPartition(new GlobalState(tableInfo.getTableArn(), Optional.of(tableInfo.getMetadata().toMap())));

            Instant startTime = Instant.now();

            if (tableInfo.getMetadata().isExportRequired()) {
//                exportTime = Instant.now();
                createExportPartition(tableInfo.getTableArn(), startTime, tableInfo.getMetadata().getExportBucket(), tableInfo.getMetadata().getExportPrefix());
            }

            if (tableInfo.getMetadata().isStreamRequired()) {
                List<String> shardIds;
                // start position by default is beginning if not provided.
                if (tableInfo.getMetadata().isExportRequired() || "LATEST".equals(tableInfo.getMetadata().getStreamStartPosition())) {
                    // For a continued data extraction process that involves both export and stream
                    // The export must be completed and loaded before stream can start.
                    // Moreover, there should not be any gaps between the export time and the time start reading the stream
                    // The design here is to start reading from the beginning of current active shards
                    // and then check if the change event datetime is greater than the export time.
                    shardIds = shardManager.getActiveShards(tableInfo.getMetadata().getStreamArn());
                    shardIds.forEach(shardId -> {
                        createStreamPartition(tableInfo.getMetadata().getStreamArn(), shardId, startTime, tableInfo.getMetadata().isExportRequired());
                    });
                } else {
                    shardIds = shardManager.getRootShardIds(tableInfo.getMetadata().getStreamArn());
                    shardIds.forEach(shardId -> {
                        createStreamPartition(tableInfo.getMetadata().getStreamArn(), shardId, null, false);
                    });
                }
            }
        });
        // Mark initialization as done, so that it won't be triggered again.
        coordinator.completePartition(initPartition.get());
    }


    /**
     * Create a partition for an export job in the coordination table. The bucket and prefix will be stored in the progress state.
     * This is to support that different tables can use different destinations.
     *
     * @param tableArn   Table Arn
     * @param exportTime Export Time
     * @param bucket     Export bucket
     * @param prefix     Export Prefix
     */
    private void createExportPartition(String tableArn, Instant exportTime, String bucket, String prefix) {
        ExportProgressState exportProgressState = new ExportProgressState();
        exportProgressState.setBucket(bucket);
        exportProgressState.setPrefix(prefix);
        exportProgressState.setExportTime(exportTime.toString()); // information purpose
        ExportPartition exportPartition = new ExportPartition(tableArn, exportTime, Optional.of(exportProgressState));
        coordinator.createPartition(exportPartition);
    }


    /**
     * Create a partition for a stream job in the coordination table.
     *
     * @param streamArn  Stream Arn
     * @param shardId    Shard Id
     * @param exportTime the start time for change events, any change events with creation datetime before this should be ignored.
     */
    private void createStreamPartition(String streamArn, String shardId, Instant exportTime, boolean waitForExport) {
        StreamProgressState streamProgressState = new StreamProgressState();
        streamProgressState.setWaitForExport(waitForExport);
        if (exportTime != null) {
            streamProgressState.setStartTime(exportTime.toEpochMilli());
        }
        coordinator.createPartition(new StreamPartition(streamArn, shardId, Optional.of(streamProgressState)));
    }

    private String getContinuousBackupsStatus(String tableName) {
        // Validate Point in time recovery is enabled or not
        DescribeContinuousBackupsRequest req = DescribeContinuousBackupsRequest.builder()
                .tableName(tableName)
                .build();
        DescribeContinuousBackupsResponse resp = dynamoDbClient.describeContinuousBackups(req);
        return resp.continuousBackupsDescription().pointInTimeRecoveryDescription().pointInTimeRecoveryStatus().toString();
    }

    private String getTableName(String tableArn) {
        Arn arn = Arn.fromString(tableArn);
        // resourceAsString is table/xxx
        return arn.resourceAsString().substring(6);
    }

    /**
     * Conduct Metadata info for table and also perform validation on configuration.
     * Once created, the info should not be changed.
     */
    private TableInfo getTableInfo(TableConfig tableConfig) {
        String tableName = getTableName(tableConfig.getTableArn());

        // Need to call describe table to get the Key schema for table
        // The key schema will be used when adding the metadata to event.
        DescribeTableRequest req = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();
        DescribeTableResponse describeTableResult = dynamoDbClient.describeTable(req);
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

        StreamConfig.StartPosition streamStartPosition = null;

        if (tableConfig.getStreamConfig() != null) {
            // Validate if DynamoDB Stream is turn on or not
            if (describeTableResult.table().streamSpecification() == null) {
                String errorMessage = "Steam is not enabled for table " + tableConfig.getTableArn();
                LOG.error(errorMessage);
                throw new InvalidPluginConfigurationException(errorMessage);
            }
            // Validate view type of DynamoDB stream
            if (describeTableResult.table().streamSpecification() != null) {
                String viewType = describeTableResult.table().streamSpecification().streamViewTypeAsString();
                LOG.debug("The stream view type for table " + tableName + " is " + viewType);
                List<String> supportedType = List.of("NEW_IMAGE", "NEW_AND_OLD_IMAGES");
                if (!supportedType.contains(viewType)) {
                    String errorMessage = "Stream " + tableConfig.getTableArn() + " is enabled with " + viewType + ". Supported types are " + supportedType;
                    LOG.error(errorMessage);
                    throw new InvalidPluginConfigurationException(errorMessage);
                }
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
                .streamStartPosition(streamStartPosition == null ? null : streamStartPosition.name())
                .exportBucket(tableConfig.getExportConfig() == null ? null : tableConfig.getExportConfig().getS3Bucket())
                .exportPrefix(tableConfig.getExportConfig() == null ? null : tableConfig.getExportConfig().getS3Prefix())
                .build();
        return new TableInfo(tableConfig.getTableArn(), metadata);
    }


}
