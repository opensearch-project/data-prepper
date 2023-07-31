/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsDispatcher;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@DataPrepperPlugin(name = "cloudwatchlogs-sink", pluginType = Sink.class, pluginConfigurationType = CloudWatchLogsSinkConfig.class)
public class CloudWatchLogsSink extends AbstractSink<Record<Event>> {
    private final CloudWatchLogsService cloudWatchLogsService;
    private boolean isInitialized;
    @DataPrepperPluginConstructor
    public CloudWatchLogsSink(final PluginSetting pluginSetting,
                              final PluginMetrics pluginMetrics,
                              final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig,
                              final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);

        AwsConfig awsConfig = cloudWatchLogsSinkConfig.getAwsConfig();
        ThresholdConfig thresholdConfig = cloudWatchLogsSinkConfig.getThresholdConfig();

        CloudWatchLogsMetrics cloudWatchLogsMetrics = new CloudWatchLogsMetrics(pluginMetrics);
        CloudWatchLogsLimits cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(),
                thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSize(),thresholdConfig.getLogSendInterval());

        CloudWatchLogsClient cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);
        BufferFactory bufferFactory = new InMemoryBufferFactory();

        Executor executor = Executors.newCachedThreadPool();

        CloudWatchLogsDispatcher cloudWatchLogsDispatcher = CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(cloudWatchLogsClient)
                .cloudWatchLogsMetrics(cloudWatchLogsMetrics)
                .logGroup(cloudWatchLogsSinkConfig.getLogGroup())
                .logStream(cloudWatchLogsSinkConfig.getLogStream())
                .backOffTimeBase(thresholdConfig.getBackOffTime())
                .retryCount(thresholdConfig.getRetryCount())
                .executor(executor)
                .build();

        Buffer buffer = bufferFactory.getBuffer();

        cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsLimits, cloudWatchLogsDispatcher);
    }

    @Override
    public void doInitialize() {
        isInitialized = true;
    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }

        cloudWatchLogsService.processLogEvents(records);
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }
}