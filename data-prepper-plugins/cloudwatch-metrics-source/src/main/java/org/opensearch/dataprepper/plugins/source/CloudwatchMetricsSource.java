/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

import java.util.Objects;


/**
 *  An implementation of cloudwatch metrics source class to Scrape the metrics using GetMetricData API
 */
@DataPrepperPlugin(name = "cloudwatch", pluginType = Source.class, pluginConfigurationType = CloudwatchMetricsSourceConfig.class)
public class CloudwatchMetricsSource implements Source<Record<Event>>, UsesSourceCoordination {

    private final PluginMetrics pluginMetrics;

    private final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig;

    private final AwsCredentialsSupplier awsCredentialsSupplier;

    Thread cloudwatchMetricsWorkerThread;

     SourceCoordinator<CloudwatchSourceProgressState> sourceCoordinator;



    @DataPrepperPluginConstructor
    public CloudwatchMetricsSource(
            final PluginMetrics pluginMetrics,
            final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig,
            final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.cloudwatchMetricsSourceConfig = cloudwatchMetricsSourceConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.pluginMetrics = pluginMetrics;

    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return false;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        final AwsAuthenticationAdapter awsAuthenticationAdapter = new AwsAuthenticationAdapter(awsCredentialsSupplier, cloudwatchMetricsSourceConfig);
        final AwsCredentialsProvider credentialsProvider = awsAuthenticationAdapter.getCredentialsProvider();
        final CloudWatchClient cloudWatchClient = CloudWatchClient.builder()
                .region(cloudwatchMetricsSourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(credentialsProvider)
                .build();
        cloudwatchMetricsWorkerThread = new Thread(new CloudwatchMetricsWorker(cloudWatchClient,
                cloudwatchMetricsSourceConfig,
                pluginMetrics,
                sourceCoordinator,buffer));
        cloudwatchMetricsWorkerThread.start();
    }

    @Override
    public void stop() {
        cloudwatchMetricsWorkerThread.interrupt();
        if (Objects.nonNull(sourceCoordinator)) {
            sourceCoordinator.giveUpPartitions();
        }
    }

    @Override
    public <T> void setSourceCoordinator(SourceCoordinator<T> sourceCoordinator) {
        this.sourceCoordinator = (SourceCoordinator<CloudwatchSourceProgressState>) sourceCoordinator;
        sourceCoordinator.initialize();
    }

    @Override
    public Class<?> getPartitionProgressStateClass() {
        return CloudwatchSourceProgressState.class;
    }
}
