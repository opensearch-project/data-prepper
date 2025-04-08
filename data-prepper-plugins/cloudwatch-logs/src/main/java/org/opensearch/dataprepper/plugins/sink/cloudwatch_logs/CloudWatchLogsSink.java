/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsDispatcher;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.exception.InvalidBufferTypeException;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@DataPrepperPlugin(name = "cloudwatch_logs", pluginType = Sink.class, pluginConfigurationType = CloudWatchLogsSinkConfig.class)
public class CloudWatchLogsSink extends AbstractSink<Record<Event>> {
    private final CloudWatchLogsService cloudWatchLogsService;
    private DlqPushHandler dlqPushHandler = null;
    private volatile boolean isInitialized;
    @DataPrepperPluginConstructor
    public CloudWatchLogsSink(final PluginSetting pluginSetting,
                              final PluginMetrics pluginMetrics,
                              final PluginFactory pluginFactory,
                              final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig,
                              final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);

        AwsConfig awsConfig = cloudWatchLogsSinkConfig.getAwsConfig();
        ThresholdConfig thresholdConfig = cloudWatchLogsSinkConfig.getThresholdConfig();

        CloudWatchLogsMetrics cloudWatchLogsMetrics = new CloudWatchLogsMetrics(pluginMetrics);
        CloudWatchLogsLimits cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(),
                thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(),thresholdConfig.getLogSendInterval());

        if (awsConfig == null && awsCredentialsSupplier == null) {
            throw new RuntimeException("Missing awsConfig and awsCredentialsSupplier");
        }
        CloudWatchLogsClient cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);
        if (cloudWatchLogsClient == null) {
            throw new RuntimeException("cloudWatchLogsClient is null");
        }

        BufferFactory bufferFactory = null;
        if (cloudWatchLogsSinkConfig.getBufferType().equals("in_memory")) {
            bufferFactory = new InMemoryBufferFactory();
        }

        if (cloudWatchLogsSinkConfig.getDlq() != null) {
            String region = awsConfig.getAwsRegion().toString();
            String role = awsConfig.getAwsStsRoleArn();
            dlqPushHandler = new DlqPushHandler(pluginFactory, pluginSetting, pluginMetrics, cloudWatchLogsSinkConfig.getDlq(), region, role, "cloudWatchLogs");
        }

        Executor executor = Executors.newCachedThreadPool();

        CloudWatchLogsDispatcher cloudWatchLogsDispatcher = CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(cloudWatchLogsClient)
                .cloudWatchLogsMetrics(cloudWatchLogsMetrics)
                .dlqPushHandler(dlqPushHandler)
                .logGroup(cloudWatchLogsSinkConfig.getLogGroup())
                .logStream(cloudWatchLogsSinkConfig.getLogStream())
                .backOffTimeBase(thresholdConfig.getBackOffTime())
                .retryCount(thresholdConfig.getRetryCount())
                .executor(executor)
                .build();

        Buffer buffer;
        try {
            buffer = bufferFactory.getBuffer();
        } catch (NullPointerException e) {
            throw new InvalidBufferTypeException("Error loading buffer!");
        }

        cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsLimits, cloudWatchLogsDispatcher, dlqPushHandler);
    }

    @Override
    public void doInitialize() {
        isInitialized = Boolean.TRUE;
    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {
        cloudWatchLogsService.processLogEvents(records);
    }

    @Override
    public boolean isReady() {
        return isInitialized;
    }
}
