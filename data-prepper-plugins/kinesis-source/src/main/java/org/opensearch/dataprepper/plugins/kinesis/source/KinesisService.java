package org.opensearch.dataprepper.plugins.kinesis.source;

import lombok.Setter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfig;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfigSupplier;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.ConsumerStrategy;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.processor.KinesisShardRecordProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KinesisService {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisService.class);
    private static final int GRACEFUL_SHUTDOWN_WAIT_INTERVAL_SECONDS = 20;

    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;

    private final String applicationName;
    private final String tableName;
    private final String kclMetricsNamespaceName;
    private final String pipelineName;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final KinesisSourceConfig sourceConfig;
    private final KinesisAsyncClient kinesisClient;
    private final DynamoDbAsyncClient dynamoDbClient;
    private final CloudWatchAsyncClient cloudWatchClient;
    private final WorkerIdentifierGenerator workerIdentifierGenerator;

    @Setter
    private Scheduler scheduler;

    private final ExecutorService executorService;

    public KinesisService(final KinesisSourceConfig sourceConfig,
                          final KinesisClientFactory kinesisClientFactory,
                          final PluginMetrics pluginMetrics,
                          final PluginFactory pluginFactory,
                          final PipelineDescription pipelineDescription,
                          final AcknowledgementSetManager acknowledgementSetManager,
                          final KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier,
                          final WorkerIdentifierGenerator workerIdentifierGenerator
                          ){
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
        this.acknowledgementSetManager = acknowledgementSetManager;
        if (kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig().isEmpty()) {
            throw new IllegalStateException("Lease Coordination table should be provided!");
        }
        KinesisLeaseConfig kinesisLeaseConfig =
                kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig().get();
        this.tableName = kinesisLeaseConfig.getLeaseCoordinationTable().getTableName();
        this.kclMetricsNamespaceName = this.tableName;
        this.dynamoDbClient = kinesisClientFactory.buildDynamoDBClient(kinesisLeaseConfig.getLeaseCoordinationTable().getAwsRegion());
        this.kinesisClient = kinesisClientFactory.buildKinesisAsyncClient(sourceConfig.getAwsAuthenticationConfig().getAwsRegion());
        this.cloudWatchClient = kinesisClientFactory.buildCloudWatchAsyncClient(kinesisLeaseConfig.getLeaseCoordinationTable().getAwsRegion());
        this.pipelineName = pipelineDescription.getPipelineName();
        this.applicationName = pipelineName;
        this.workerIdentifierGenerator = workerIdentifierGenerator;
        this.executorService = Executors.newFixedThreadPool(1);
    }

    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (sourceConfig.getStreams() == null || sourceConfig.getStreams().isEmpty()) {
            throw new IllegalStateException("Streams are empty!");
        }

        scheduler = getScheduler(buffer);
        executorService.execute(scheduler);
    }

    public void shutDown() {
        LOG.info("Stop request received for Kinesis Source");

        Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
        LOG.info("Waiting up to {} seconds for shutdown to complete.", GRACEFUL_SHUTDOWN_WAIT_INTERVAL_SECONDS);
        try {
            gracefulShutdownFuture.get(GRACEFUL_SHUTDOWN_WAIT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            LOG.error("Exception while executing kinesis consumer graceful shutdown, doing force shutdown", ex);
            scheduler.shutdown();
        }
        LOG.info("Completed, shutting down now.");
    }

    public Scheduler getScheduler(final Buffer<Record<Event>> buffer) {
        if (scheduler == null) {
            return createScheduler(buffer);
        }
        return scheduler;
    }

    public Scheduler createScheduler(final Buffer<Record<Event>> buffer) {
        final ShardRecordProcessorFactory processorFactory = new KinesisShardRecordProcessorFactory(
                buffer, sourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory);

        ConfigsBuilder configsBuilder =
                new ConfigsBuilder(
                        new KinesisMultiStreamTracker(kinesisClient, sourceConfig, applicationName),
                        applicationName, kinesisClient, dynamoDbClient, cloudWatchClient,
                        workerIdentifierGenerator.generate(), processorFactory
                )
                .tableName(tableName)
                .namespace(kclMetricsNamespaceName);

        ConsumerStrategy consumerStrategy = sourceConfig.getConsumerStrategy();
        if (consumerStrategy == ConsumerStrategy.POLLING) {
            configsBuilder.retrievalConfig().retrievalSpecificConfig(
                new PollingConfig(kinesisClient)
                    .maxRecords(sourceConfig.getPollingConfig().getMaxPollingRecords())
                    .idleTimeBetweenReadsInMillis(
                            sourceConfig.getPollingConfig().getIdleTimeBetweenReads().toMillis()));
        }

        return new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig()
                        .billingMode(BillingMode.PAY_PER_REQUEST),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );
    }
}
