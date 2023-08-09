/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.configuration.NamespacesListConfig;
import org.opensearch.dataprepper.plugins.source.configuration.MetricDataQueriesConfig;
import org.opensearch.dataprepper.plugins.source.configuration.DimensionsListConfig;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import java.time.Duration;
import java.util.Collection;

import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.ScanBy;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;


/**
 *  An implementation of cloudwatch metrics source class to Scrape the metrics using GetMetricData API
 */
@DataPrepperPlugin(name = "cloudwatch", pluginType = Source.class, pluginConfigurationType = CloudwatchMetricsSourceConfig.class)
public class CloudwatchMetricsSource implements Source<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(CloudwatchMetricsSource.class);
    private final Collection<Dimension> dimensionCollection;
    private final CloudwatchMetricsWorker cloudwatchMetricsWorker;
    private final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(30);
    @DataPrepperPluginConstructor
    public CloudwatchMetricsSource(
            final PluginMetrics pluginMetrics,
            final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig,
            final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.cloudwatchMetricsSourceConfig = cloudwatchMetricsSourceConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        cloudwatchMetricsWorker = new CloudwatchMetricsWorker();
        dimensionCollection = new ArrayList<>();
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

        BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, cloudwatchMetricsSourceConfig.getBatchSize(), BUFFER_TIMEOUT);

        final AwsAuthenticationAdapter awsAuthenticationAdapter = new AwsAuthenticationAdapter(awsCredentialsSupplier, cloudwatchMetricsSourceConfig);
        final AwsCredentialsProvider credentialsProvider = awsAuthenticationAdapter.getCredentialsProvider();

        CloudWatchClient cloudWatchClient = CloudWatchClient.builder()
                .region(cloudwatchMetricsSourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(credentialsProvider)
                .build();

        for (NamespacesListConfig namespacesListConfig : cloudwatchMetricsSourceConfig.getNamespacesListConfig()) {
            dimensionCollection.clear();
            for (MetricDataQueriesConfig metricDataQueriesConfig : namespacesListConfig.getNamespaceConfig().getMetricDataQueriesConfig()) {

                for (DimensionsListConfig dimensionsListConfig : metricDataQueriesConfig.getMetricsConfig().getDimensionsListConfigs()) {

                    dimensionCollection.add(Dimension.builder()
                            .name(dimensionsListConfig.getDimensionConfig().getName())
                            .value(dimensionsListConfig.getDimensionConfig().getValue()).build());
                }
                try {

                    Metric met = Metric.builder()
                            .metricName(metricDataQueriesConfig.getMetricsConfig().getName())
                            .namespace(namespacesListConfig.getNamespaceConfig().getName())
                            .dimensions(dimensionCollection)
                            .build();

                    MetricStat metStat = MetricStat.builder()
                            .stat(metricDataQueriesConfig.getMetricsConfig().getStat())
                            .unit(metricDataQueriesConfig.getMetricsConfig().getUnit())
                            .period(metricDataQueriesConfig.getMetricsConfig().getPeriod())
                            .metric(met)
                            .build();

                    MetricDataQuery dataQUery = MetricDataQuery.builder()
                            .metricStat(metStat)
                            .id(metricDataQueriesConfig.getMetricsConfig().getId())
                            .returnData(true)
                            .build();

                    List<MetricDataQuery> dataQueries = new ArrayList<>();
                    dataQueries.add(dataQUery);

                    GetMetricDataRequest getMetReq = GetMetricDataRequest.builder()
                            .maxDatapoints(10)
                            .scanBy(ScanBy.TIMESTAMP_DESCENDING)
                            .startTime(Instant.parse(namespacesListConfig.getNamespaceConfig().getStartTime()))
                            .endTime(Instant.parse(namespacesListConfig.getNamespaceConfig().getEndTime()))
                            .metricDataQueries(dataQueries)
                            .build();

                    GetMetricDataResponse response = cloudWatchClient.getMetricData(getMetReq);
                    List<MetricDataResult> data = response.metricDataResults();
                    cloudwatchMetricsWorker.writeToBuffer(data, bufferAccumulator, null);

                } catch (CloudWatchException | DateTimeParseException ex) {
                    LOG.error("Exception Occurred while scraping the metrics  {0}", ex);
                }
            }
        }
    }
    @Override
    public void stop() {
        LOG.info("Stopped Cloudwatch source.");
    }
}
