/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileLoaderFactory;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.ManifestFileReader;
import org.opensearch.dataprepper.plugins.source.dynamodb.export.S3ObjectReader;
import org.opensearch.dataprepper.plugins.source.dynamodb.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.dynamodb.leader.ShardManager;
import org.opensearch.dataprepper.plugins.source.dynamodb.stream.ShardConsumerFactory;
import org.opensearch.dataprepper.plugins.source.dynamodb.stream.StreamScheduler;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.BackoffCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DynamoDBService {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBService.class);

    private final List<TableConfig> tableConfigs;

    private final EnhancedSourceCoordinator coordinator;

    private final DynamoDbClient dynamoDbClient;

    private final DynamoDBSourceConfig dynamoDBSourceConfig;
    //
    private final DynamoDbStreamsClient dynamoDbStreamsClient;

    private final S3Client s3Client;

    private final ShardManager shardManager;

    private final ExecutorService executor;

    private final PluginMetrics pluginMetrics;

    private final AcknowledgementSetManager acknowledgementSetManager;


    public DynamoDBService(final EnhancedSourceCoordinator coordinator,
                           final ClientFactory clientFactory,
                           final DynamoDBSourceConfig sourceConfig,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        this.coordinator = coordinator;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.dynamoDBSourceConfig = sourceConfig;

        // Initialize AWS clients
        dynamoDbClient = clientFactory.buildDynamoDBClient();
        dynamoDbStreamsClient = clientFactory.buildDynamoDbStreamClient();
        s3Client = clientFactory.buildS3Client();

        // A shard manager is responsible to retrieve the shard information from streams.
        shardManager = new ShardManager(dynamoDbStreamsClient);
        tableConfigs = sourceConfig.getTableConfigs();
        executor = Executors.newFixedThreadPool(4);
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
        Runnable fileLoaderScheduler = new DataFileScheduler(coordinator, loaderFactory, pluginMetrics, acknowledgementSetManager, dynamoDBSourceConfig);

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, dynamoDbStreamsClient, pluginMetrics, buffer);
        Runnable streamScheduler = new StreamScheduler(coordinator, consumerFactory, pluginMetrics, acknowledgementSetManager, dynamoDBSourceConfig, new BackoffCalculator());
        // leader scheduler will handle the initialization
        Runnable leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, tableConfigs);

        // May consider start or shutdown the scheduler on demand
        // Currently, event after the exports are done, the related scheduler will not be shutdown
        // This is because in the future we may support incremental exports.
        executor.submit(leaderScheduler);
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

}
