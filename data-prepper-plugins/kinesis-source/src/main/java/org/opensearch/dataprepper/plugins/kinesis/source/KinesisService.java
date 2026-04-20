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

import com.amazonaws.SdkClientException;
import com.linecorp.armeria.client.retry.Backoff;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfig;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfigSupplier;
import org.opensearch.dataprepper.plugins.kinesis.source.apihandler.KinesisClientApiHandler;
import org.opensearch.dataprepper.plugins.kinesis.source.apihandler.KinesisClientApiRetryHandler;
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
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

public class KinesisService {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisService.class);
    private static final int GRACEFUL_SHUTDOWN_WAIT_INTERVAL_SECONDS = 20;
    private static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();
    private static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();
    private static final double JITTER_RATE = 0.20;
    private static final int NUM_OF_RETRIES = 3;

    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;

    @Getter
    private final String applicationName;

    private final String tableName;
    private final String kclMetricsNamespaceName;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final KinesisAsyncClient kinesisClient;
    private final DynamoDbAsyncClient dynamoDbClient;
    private final CloudWatchAsyncClient cloudWatchClient;
    private final WorkerIdentifierGenerator workerIdentifierGenerator;
    private final InputCodec codec;

    @Setter
    private Scheduler scheduler;
    private final ExecutorService executorService;

    public KinesisService(final KinesisSourceConfig kinesisSourceConfig,
                          final KinesisClientFactory kinesisClientFactory,
                          final PluginMetrics pluginMetrics,
                          final PluginFactory pluginFactory,
                          final PipelineDescription pipelineDescription,
                          final AcknowledgementSetManager acknowledgementSetManager,
                          final KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier,
                          final WorkerIdentifierGenerator workerIdentifierGenerator
                          ){
        this.kinesisSourceConfig = kinesisSourceConfig;
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
        this.kinesisClient = kinesisClientFactory.buildKinesisAsyncClient(kinesisSourceConfig.getAwsAuthenticationConfig().getAwsRegion());
        this.cloudWatchClient = kinesisClientFactory.buildCloudWatchAsyncClient(kinesisLeaseConfig.getLeaseCoordinationTable().getAwsRegion());
        final String pipelineIdentifier = kinesisLeaseConfig.getPipelineIdentifier();
        if (Objects.isNull(pipelineIdentifier) || pipelineIdentifier.isEmpty()) {
            this.applicationName = pipelineDescription.getPipelineName();
        } else {
            this.applicationName = kinesisLeaseConfig.getPipelineIdentifier();
            }
        this.workerIdentifierGenerator = workerIdentifierGenerator;
        this.executorService = Executors.newFixedThreadPool(1);
        final PluginModel codecConfiguration = kinesisSourceConfig.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
        this.codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
    }

    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null.");
        }

        if (kinesisSourceConfig.getStreams() == null || kinesisSourceConfig.getStreams().isEmpty()) {
            throw new InvalidPluginConfigurationException("No Kinesis streams provided.");
        }

        scheduler = getScheduler(buffer);
        executorService.execute(scheduler);
    }

    public void shutDown() {
        LOG.info("Stop request received for Kinesis Source");

        if (scheduler != null) {
            Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
            LOG.info("Waiting up to {} seconds for shutdown to complete.", GRACEFUL_SHUTDOWN_WAIT_INTERVAL_SECONDS);
            try {
                gracefulShutdownFuture.get(GRACEFUL_SHUTDOWN_WAIT_INTERVAL_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                LOG.error("Exception while executing kinesis consumer graceful shutdown, doing force shutdown", ex);
                scheduler.shutdown();
            }
            LOG.info("Completed, shutting down now.");
        } else {
            LOG.info("The Kinesis Scheduler was not initialized.");
        }
    }

    public Scheduler getScheduler(final Buffer<Record<Event>> buffer) {
        int maxAttempts = kinesisSourceConfig.getMaxInitializationAttempts();
        while (scheduler == null && maxAttempts-- > 0 ) {
            try {
                scheduler = createScheduler(buffer);
            } catch (SdkClientException | KinesisClientLibDependencyException | ThrottlingException ex) {
                LOG.error(NOISY, "Caught exception when initializing KCL Scheduler due to {}. Number of remaining retries: {}", ex.getMessage(), maxAttempts);
            } catch (Exception ex) {
                LOG.error(NOISY, "Caught exception when initializing KCL Scheduler. Number of remaining retries: {}", maxAttempts, ex);
            }

            if (scheduler == null) {
                try {
                    Thread.sleep(kinesisSourceConfig.getInitializationBackoffTime().toMillis());
                } catch (InterruptedException e){
                    LOG.debug("Interrupted exception.");
                }
            }
        }
        return scheduler;
    }

    public Scheduler createScheduler(final Buffer<Record<Event>> buffer) {
        final ShardRecordProcessorFactory processorFactory = new KinesisShardRecordProcessorFactory(
                buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec);

        ConfigsBuilder configsBuilder =
                new ConfigsBuilder(
                        new KinesisMultiStreamTracker(kinesisSourceConfig, applicationName, new KinesisClientApiHandler(kinesisClient, new KinesisClientApiRetryHandler(Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                                .withMaxAttempts(NUM_OF_RETRIES), NUM_OF_RETRIES))),
                        applicationName, kinesisClient, dynamoDbClient, cloudWatchClient,
                        workerIdentifierGenerator.generate(), processorFactory
                )
                .tableName(tableName)
                .namespace(kclMetricsNamespaceName);

        RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig();
        ConsumerStrategy consumerStrategy = kinesisSourceConfig.getConsumerStrategy();
        if (consumerStrategy == ConsumerStrategy.POLLING) {
            retrievalConfig = configsBuilder.retrievalConfig().retrievalSpecificConfig(
                new PollingConfig(kinesisClient)
                    .maxRecords(kinesisSourceConfig.getPollingConfig().getMaxPollingRecords())
                    .idleTimeBetweenReadsInMillis(
                            kinesisSourceConfig.getPollingConfig().getIdleTimeBetweenReads().toMillis()));
        }

        MetricsConfig metricsConfig = kinesisSourceConfig.isKclMetricsEnabled()
                ? configsBuilder.metricsConfig()
                : configsBuilder.metricsConfig().metricsFactory(new NullMetricsFactory());

        return new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig()
                        .schedulerInitializationBackoffTimeMillis(kinesisSourceConfig.getInitializationBackoffTime().toMillis())
                        .maxInitializationAttempts(kinesisSourceConfig.getMaxInitializationAttempts()),
                configsBuilder.leaseManagementConfig().billingMode(BillingMode.PAY_PER_REQUEST),
                configsBuilder.lifecycleConfig(),
                metricsConfig,
                configsBuilder.processorConfig(),
                retrievalConfig
        );
    }
}
