/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
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
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.EntityResolver;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig;

import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.exception.InvalidBufferTypeException;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsSinkUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Experimental
@DataPrepperPlugin(name = "cloudwatch_logs", pluginType = Sink.class, pluginConfigurationType = CloudWatchLogsSinkConfig.class)
public class CloudWatchLogsSink extends AbstractSink<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsSink.class);

    // Also keeps the cardinality gauge's weak reference to the service from being collected.
    private final CloudWatchLogsService cloudWatchLogsService;
    private DlqPushHandler dlqPushHandler = null;
    private volatile boolean isInitialized;

    @DataPrepperPluginConstructor
    public CloudWatchLogsSink(final PluginSetting pluginSetting,
                              final PluginMetrics pluginMetrics,
                              final PluginFactory pluginFactory,
                              final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig,
                              final AwsCredentialsSupplier awsCredentialsSupplier,
                              final ExpressionEvaluator expressionEvaluator) {
        super(pluginSetting);

        AwsConfig awsConfig = cloudWatchLogsSinkConfig.getAwsConfig();
        ThresholdConfig thresholdConfig = cloudWatchLogsSinkConfig.getThresholdConfig();
        Map<String, String> headerOverrides = cloudWatchLogsSinkConfig.getHeaderOverrides();

        // Literal headers are applied once at client build; templated headers are resolved per request and
        // must be kept off the client, or they would be sent verbatim.
        final Map<String, String> staticHeaders = new HashMap<>();
        final Map<String, String> dynamicHeaderTemplates = new LinkedHashMap<>();
        for (final Map.Entry<String, String> header : headerOverrides.entrySet()) {
            if (CloudWatchLogsSinkUtils.isDynamicExpression(header.getValue(), expressionEvaluator)) {
                dynamicHeaderTemplates.put(header.getKey(), header.getValue());
            } else {
                staticHeaders.put(header.getKey(), header.getValue());
            }
        }

        // Log custom headers configuration during plugin startup
        logCustomHeadersConfiguration(headerOverrides);

        CloudWatchLogsMetrics cloudWatchLogsMetrics = new CloudWatchLogsMetrics(pluginMetrics);
        CloudWatchLogsLimits cloudWatchLogsLimits = new CloudWatchLogsLimits(thresholdConfig.getBatchSize(),
                thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(), thresholdConfig.getFlushInterval());

        if (awsConfig == null && awsCredentialsSupplier == null) {
            throw new RuntimeException("Missing awsConfig and awsCredentialsSupplier");
        }
        CloudWatchLogsClient cloudWatchLogsClient = CloudWatchLogsClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier, staticHeaders, cloudWatchLogsSinkConfig.getEndpoint());
        if (cloudWatchLogsClient == null) {
            throw new RuntimeException("cloudWatchLogsClient is null");
        }

        BufferFactory bufferFactory = null;
        bufferFactory = new InMemoryBufferFactory();

        if (cloudWatchLogsSinkConfig.getDlq() != null) {
            String region = null;
            if (awsConfig != null && awsConfig.getAwsRegion() != null) {
                region = awsConfig.getAwsRegion().toString();
            } else if (awsCredentialsSupplier != null) {
                region = awsCredentialsSupplier.getDefaultRegion()
                        .map(Region::toString)
                        .orElse(null);
            }
            String role = null;
            if (awsConfig != null && awsConfig.getAwsStsRoleArn() != null) {
                role = awsConfig.getAwsStsRoleArn();
            } else if (awsCredentialsSupplier != null) {
                role = awsCredentialsSupplier.getDefaultStsRoleArn()
                        .orElse(null);
            }
            dlqPushHandler = new DlqPushHandler(pluginFactory, pluginSetting, pluginMetrics, cloudWatchLogsSinkConfig.getDlq(), region, role, "cloudWatchLogs");
        }

        Executor executor = Executors.newFixedThreadPool(cloudWatchLogsSinkConfig.getWorkers(),
                BackgroundThreadFactory.defaultExecutorThreadFactory("cloudwatch-logs-sink"));

        final EntityConfig entityConfig = cloudWatchLogsSinkConfig.getEntityConfig();
        final boolean dynamicEntity = entityConfig != null && entityConfig.isDynamic(expressionEvaluator);
        final boolean dynamicHeaders = !dynamicHeaderTemplates.isEmpty();
        // Events are partitioned into per-group requests whenever the entity or the headers vary per event.
        final boolean grouped = dynamicEntity || dynamicHeaders;

        final Entity staticEntity = (entityConfig == null || dynamicEntity) ? null : Entity.builder()
                .keyAttributes(entityConfig.getKeyAttributes())
                .attributes(entityConfig.getAttributes())
                .build();

        // The same evaluator that classified this config as dynamic has to do the interpolating, or
        // expression-valued attributes would resolve to an empty string instead of being evaluated. Key and
        // attribute templates are only supplied when the entity itself is dynamic; otherwise the resolver
        // partitions purely by the dynamic headers and uses staticEntity for every group.
        final EntityResolver entityResolver = grouped
                ? new EntityResolver(
                        dynamicEntity ? entityConfig.getKeyAttributes() : Map.of(),
                        dynamicEntity ? entityConfig.getAttributes() : Map.of(),
                        dynamicHeaderTemplates,
                        expressionEvaluator)
                : null;
        final int maxEntityCardinality = entityConfig != null
                ? entityConfig.getMaxCardinality()
                : EntityConfig.DEFAULT_MAX_CARDINALITY;

        CloudWatchLogsDispatcher cloudWatchLogsDispatcher = CloudWatchLogsDispatcher.builder()
                .cloudWatchLogsClient(cloudWatchLogsClient)
                .cloudWatchLogsMetrics(cloudWatchLogsMetrics)
                .dlqPushHandler(dlqPushHandler)
                .dropIfDlqNotConfigured(true)
                .logGroup(cloudWatchLogsSinkConfig.getLogGroup())
                .logStream(cloudWatchLogsSinkConfig.getLogStream())
                .retryCount(dlqPushHandler == null ? Integer.MAX_VALUE : cloudWatchLogsSinkConfig.getMaxRetries())
                .executor(executor)
                .createLogGroup(cloudWatchLogsSinkConfig.getCreateLogGroup())
                .createLogStream(cloudWatchLogsSinkConfig.getCreateLogStream())
                // In grouped mode the per-group entity is the sole source of truth; the dispatcher's frozen
                // entity is only used by the non-grouped static path.
                .entity(grouped ? null : staticEntity)
                .build();

        if (grouped) {
            cloudWatchLogsService = new CloudWatchLogsService(bufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                    cloudWatchLogsDispatcher, dlqPushHandler, true, entityResolver,
                    maxEntityCardinality, staticEntity);
            // Gauged on the service: the group count is the number of groups actually being buffered
            // right now, and it falls again as groups go idle rather than only ever climbing.
            cloudWatchLogsMetrics.registerEntityCardinalityGauge(cloudWatchLogsService,
                    CloudWatchLogsService::activeEntityGroupCount);
        } else {
            Buffer buffer;
            try {
                buffer = bufferFactory.getBuffer();
            } catch (NullPointerException e) {
                throw new InvalidBufferTypeException("Error loading buffer!");
            }
            cloudWatchLogsService = new CloudWatchLogsService(buffer, cloudWatchLogsMetrics, cloudWatchLogsLimits,
                    cloudWatchLogsDispatcher, dlqPushHandler, true);
        }
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

    /**
     * Logs custom headers configuration during plugin startup.
     * Ensures no sensitive header values are logged.
     *
     * @param headerOverrides The custom headers map to log
     */
    private void logCustomHeadersConfiguration(Map<String, String> headerOverrides) {
        if (LOG.isInfoEnabled()) {
            if (headerOverrides.isEmpty()) {
                LOG.info("CloudWatch Logs sink initialized without custom headers");
            } else {
                int headerCount = headerOverrides.size();
                String headerNames = String.join(", ", headerOverrides.keySet());

                LOG.info("CloudWatch Logs sink initialized with {} custom headers: [{}]",
                        headerCount, headerNames);
            }
        }
    }
}
