/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.configuration.DimensionsListConfig;
import org.opensearch.dataprepper.plugins.source.configuration.MetricsConfig;
import org.opensearch.dataprepper.plugins.source.configuration.NamespaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.cloudwatch.model.ScanBy;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * implements the CloudwatchMetricsWorker to read and metrics data message and push to buffer.
 */

/**
 * An implementation of cloudwatch metrics source  worker class to write the metric to Buffer
 */
public class CloudwatchMetricsWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CloudwatchMetricsWorker.class);

    private static final int STANDARD_BACKOFF_MILLIS = 30_000;

    private final CloudWatchClient cloudWatchClient;

    private final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig;

    private final PluginMetrics pluginMetrics;

    private final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier;

    private final SourceCoordinator<CloudwatchSourceProgressState> sourceCoordinator;

    private final Buffer<Record<Event>> buffer;

    private Map<String, Collection<Dimension>> metricNameToDimension;

    private static final String NAMESPACE = "NAMESPACE";

    private static final String METRIC_NAME = "METRIC_NAME";

    public CloudwatchMetricsWorker(final CloudWatchClient cloudWatchClient,
                                   final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig,
                                   final PluginMetrics pluginMetrics,
                                   final SourceCoordinator<CloudwatchSourceProgressState> sourceCoordinator,
                                   final Buffer<Record<Event>> buffer) {
        this.cloudWatchClient = cloudWatchClient;
        this.cloudwatchMetricsSourceConfig = cloudwatchMetricsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.sourceCoordinator = sourceCoordinator;
        final List<String> namespaceMetricNames = cloudwatchMetricsSourceConfig.getNamespaceConfigs().stream()
                .flatMap(namespaceConfig -> namespaceConfig.getMetricsConfigs()
                        .stream().map(metricName -> namespaceConfig.getName() + "@" + metricName.getName())).collect(Collectors.toList());
        this.partitionCreationSupplier = new CloudwatchPartitionCreationSupplier(namespaceMetricNames);
        this.buffer = buffer;
        metricNameToDimension = getMetricNameToDimension();
    }

    void startProcessingCloudwatchMetrics(final int waitTimeMillis) {
        final Optional<SourcePartition<CloudwatchSourceProgressState>> objectToProcess = getCloudwatchSourceProgressStateSourcePartition(waitTimeMillis);
        if (objectToProcess == null) {
            return;
        }

        final Map<String, String> metricData = getMetricData(objectToProcess.get());

        final String nameSpaceName = metricData.get(NAMESPACE);
        final String metricName = metricData.get(METRIC_NAME);

        try {
            processCloudwatchMetrics(nameSpaceName, metricName);
            sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey(), false);
        } catch (final PartitionNotOwnedException | PartitionNotFoundException | PartitionUpdateException e) {
            LOG.warn("Cloud watch source received an exception from the source coordinator. There is a potential for duplicate data from {}, giving up partition and getting next partition: {}", objectToProcess.get().getPartitionKey(), e.getMessage());
            sourceCoordinator.giveUpPartitions();
        }
    }

    private Map<String, String> getMetricData(SourcePartition<CloudwatchSourceProgressState> sourceProgressStateSourcePartition) {
        Map<String, String> metricDataMap = new HashMap<>();
        metricDataMap.put(NAMESPACE, sourceProgressStateSourcePartition.getPartitionKey().split("@")[0]);
        metricDataMap.put(METRIC_NAME, sourceProgressStateSourcePartition.getPartitionKey().split("@")[1]);
        return metricDataMap;
    }

    private Optional<SourcePartition<CloudwatchSourceProgressState>> getCloudwatchSourceProgressStateSourcePartition(int waitTimeMillis) {
        final Optional<SourcePartition<CloudwatchSourceProgressState>> objectToProcess = sourceCoordinator.getNextPartition(partitionCreationSupplier);

        if (objectToProcess.isEmpty()) {
            try {
                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                LOG.error("Thread Interrupted", e);
            }
            return null;
        }
        return objectToProcess;
    }

    protected void processCloudwatchMetrics(final String nameSpaceName, final String metricName) {
        for (NamespaceConfig namespaceConfig : cloudwatchMetricsSourceConfig.getNamespaceConfigs().stream()
                .filter(obj -> obj.getName().equals(nameSpaceName)).collect(Collectors.toList())) {
            for (MetricsConfig metricsConfig : namespaceConfig.getMetricsConfigs().stream().
                    filter(obj -> obj.getName().equals(metricName)).collect(Collectors.toList())) {


                try {
                    Metric met = Metric.builder()
                            .metricName(metricsConfig.getName())
                            .namespace(namespaceConfig.getName())
                            .dimensions(metricNameToDimension.get(metricsConfig.getName()))
                            .build();

                    MetricStat metStat = MetricStat.builder()
                            .stat(metricsConfig.getStat())
                            .unit(metricsConfig.getUnit())
                            .period(metricsConfig.getPeriod())
                            .metric(met)
                            .build();

                    MetricDataQuery dataQuery = MetricDataQuery.builder()
                            .metricStat(metStat)
                            .id(metricsConfig.getId())
                            .returnData(true)
                            .build();

                    List<MetricDataQuery> dataQueries = new ArrayList<>();
                    dataQueries.add(dataQuery);

                    GetMetricDataRequest getMetReq = GetMetricDataRequest.builder()
                            .maxDatapoints(10)
                            .scanBy(ScanBy.TIMESTAMP_DESCENDING)
                            .startTime(Instant.parse(namespaceConfig.getStartTime()))
                            .endTime(Instant.parse(namespaceConfig.getEndTime()))
                            .metricDataQueries(dataQueries)
                            .build();

                    GetMetricDataResponse response = cloudWatchClient.getMetricData(getMetReq);
                    List<MetricDataResult> data = response.metricDataResults();
                    writeToBuffer(data, buffer, null);

                } catch (CloudWatchException | DateTimeParseException ex) {
                    LOG.error("Exception Occurred while scraping the metrics  {0}", ex);
                }
            }
        }
    }

    /**
     * Helps to write metrics data to buffer and to send end to end acknowledgements after successful processing
     *
     * @param metricsData        metricsData
     * @param buffer             buffer
     * @param acknowledgementSet acknowledgementSet
     */
    public void writeToBuffer(final List<MetricDataResult> metricsData,
                              final Buffer<Record<Event>> buffer,
                              final AcknowledgementSet acknowledgementSet) {
        metricsData.forEach(message -> {
            final Record<Event> eventRecord = new Record<Event>(JacksonEvent.fromMessage(message.toString()));
            try {

                final Integer timeoutInMillis = Math.toIntExact(cloudwatchMetricsSourceConfig.getBufferTimeout().toMillis());
                buffer.writeAll(List.of(eventRecord), timeoutInMillis);
            } catch (Exception ex) {
                LOG.error("Exception while adding record events {0}", ex);
            }
            if (Objects.nonNull(acknowledgementSet)) {
                acknowledgementSet.add(eventRecord.getData());
            }
        });

    }

    @Override
    public void run() {
        startProcessingCloudwatchMetrics(STANDARD_BACKOFF_MILLIS);
    }


    private Map<String, Collection<Dimension>> getMetricNameToDimension() {
        Map<String, Collection<Dimension>> metricNameToDimensionMap = new HashMap<>();
        final Optional<SourcePartition<CloudwatchSourceProgressState>> sourcePartitionInfo = getCloudwatchSourceProgressStateSourcePartition(STANDARD_BACKOFF_MILLIS);
        if (!sourcePartitionInfo.isPresent()) {
            throw new RuntimeException("No source partition info found");
        }
        final Map<String, String> metricData = getMetricData(sourcePartitionInfo.get());
        final String nameSpaceName = metricData.get("NAMESPACE");
        final String metricName = metricData.get("METRIC_NAME");

        final Collection<Dimension> dimensionCollection = new ArrayList<>();
        for (NamespaceConfig namespaceConfig : cloudwatchMetricsSourceConfig.getNamespaceConfigs().stream()
                .filter(obj -> obj.getName().equals(nameSpaceName)).collect(Collectors.toList())) {

            for (MetricsConfig metricsConfig : namespaceConfig.getMetricsConfigs().stream().
                    filter(obj -> obj.getName().equals(metricName)).collect(Collectors.toList())) {

                for (DimensionsListConfig dimensionsListConfig : metricsConfig.getDimensionsListConfigs()) {
                    dimensionCollection.add(Dimension.builder()
                            .name(dimensionsListConfig.getDimensionConfig().getName())
                            .value(dimensionsListConfig.getDimensionConfig().getValue()).build());
                }
                if (metricNameToDimensionMap.get(metricsConfig.getName()) == null) {
                    metricNameToDimensionMap.put(metricsConfig.getName(), dimensionCollection);
                } else {
                    dimensionCollection.addAll(metricNameToDimensionMap.get(metricsConfig.getName()));
                    metricNameToDimensionMap.put(metricsConfig.getName(),
                            dimensionCollection);
                }
            }
        }
        return metricNameToDimensionMap;
    }
}
