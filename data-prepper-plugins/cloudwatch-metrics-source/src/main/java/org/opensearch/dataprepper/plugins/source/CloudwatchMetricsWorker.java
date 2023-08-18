/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
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
import org.opensearch.dataprepper.plugins.source.configuration.MetricDataQueriesConfig;
import org.opensearch.dataprepper.plugins.source.configuration.NamespacesListConfig;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *  implements the CloudwatchMetricsWorker to read and metrics data message and push to buffer.
 */
/**
 *  An implementation of cloudwatch metrics source  worker class to write the metric to Buffer
 */
public class CloudwatchMetricsWorker implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(CloudwatchMetricsWorker.class);

    private static final int STANDARD_BACKOFF_MILLIS = 30_000;

    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    private final CloudWatchClient cloudWatchClient;

    private final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig;

    private final Collection<Dimension> dimensionCollection;

    private final PluginMetrics pluginMetrics;

    private final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier;

    private final SourceCoordinator<CloudwatchSourceProgressState> sourceCoordinator;

    public CloudwatchMetricsWorker(final CloudWatchClient cloudWatchClient,
                                   final BufferAccumulator<Record<Event>> bufferAccumulator,
                                   final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig,
                                   final Collection<Dimension> dimensionCollection,
                                   final PluginMetrics pluginMetrics,
                                   final SourceCoordinator<CloudwatchSourceProgressState> sourceCoordinator) {
        this.bufferAccumulator = bufferAccumulator;
        this.cloudWatchClient = cloudWatchClient;
        this.cloudwatchMetricsSourceConfig = cloudwatchMetricsSourceConfig;
        this.dimensionCollection = dimensionCollection;
        this.pluginMetrics = pluginMetrics;
        this.sourceCoordinator = sourceCoordinator;
        final List<String> namespaceMetricNames = cloudwatchMetricsSourceConfig.getNamespacesListConfig().stream()
                .map(NamespacesListConfig::getNamespaceConfig).flatMap(namespaceConfig -> namespaceConfig.getMetricDataQueriesConfig()
                        .stream().map(metricName -> namespaceConfig.getName() + "@" + metricName.getMetricsConfig().getName())).collect(Collectors.toList());
        this.partitionCreationSupplier = new CloudwatchPartitionCreationSupplier(namespaceMetricNames);
    }

    void startProcessingCloudwatchMetrics(final int waitTimeMillis){
        final Optional<SourcePartition<CloudwatchSourceProgressState>> objectToProcess = sourceCoordinator.getNextPartition(partitionCreationSupplier);

        if (objectToProcess.isEmpty()) {
            try {
                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }
        final String nameSpaceName = objectToProcess.get().getPartitionKey().split("@")[0];
        final String metricName = objectToProcess.get().getPartitionKey().split("@")[1];
        try {
            processCloudwatchMetrics(nameSpaceName,metricName);
            sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey());
        } catch (final PartitionNotOwnedException | PartitionNotFoundException | PartitionUpdateException e) {
            LOG.warn("Cloud watch source received an exception from the source coordinator. There is a potential for duplicate data from {}, giving up partition and getting next partition: {}",objectToProcess.get().getPartitionKey() , e.getMessage());
            sourceCoordinator.giveUpPartitions();
        }
    }

    protected void processCloudwatchMetrics(final String nameSpaceName, final String metricName) {
        for(NamespacesListConfig namespacesListConfig : cloudwatchMetricsSourceConfig.getNamespacesListConfig().stream()
                .filter(obj -> obj.getNamespaceConfig().getName().equals(nameSpaceName)).collect(Collectors.toList())) {
            dimensionCollection.clear();
            for (MetricDataQueriesConfig metricDataQueriesConfig : namespacesListConfig.getNamespaceConfig().getMetricDataQueriesConfig().stream().
                    filter(obj -> obj.getMetricsConfig().getName().equals(metricName)).collect(Collectors.toList())) {

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

                    MetricDataQuery dataQuery = MetricDataQuery.builder()
                            .metricStat(metStat)
                            .id(metricDataQueriesConfig.getMetricsConfig().getId())
                            .returnData(true)
                            .build();

                    List<MetricDataQuery> dataQueries = new ArrayList<>();
                    dataQueries.add(dataQuery);

                    GetMetricDataRequest getMetReq = GetMetricDataRequest.builder()
                            .maxDatapoints(10)
                            .scanBy(ScanBy.TIMESTAMP_DESCENDING)
                            .startTime(Instant.parse(namespacesListConfig.getNamespaceConfig().getStartTime()))
                            .endTime(Instant.parse(namespacesListConfig.getNamespaceConfig().getEndTime()))
                            .metricDataQueries(dataQueries)
                            .build();

                    GetMetricDataResponse response = cloudWatchClient.getMetricData(getMetReq);
                    List<MetricDataResult> data = response.metricDataResults();
                    writeToBuffer(data, bufferAccumulator, null);

                } catch (CloudWatchException | DateTimeParseException ex) {
                    LOG.error("Exception Occurred while scraping the metrics  {0}", ex);
                }
            }
        }
    }

    /**
     * Helps to write metrics data to buffer and to send end to end acknowledgements after successful processing
     * @param metricsData metricsData
     * @param bufferAccumulator bufferAccumulator
     * @param acknowledgementSet acknowledgementSet
     */
    public void writeToBuffer(final List<MetricDataResult> metricsData,
                              final BufferAccumulator<Record<Event>> bufferAccumulator,
                              final AcknowledgementSet acknowledgementSet) {
        metricsData.forEach(message -> {
            final Record<Event> eventRecord = new Record<Event>(JacksonEvent.fromMessage(message.toString()));
            try {
                bufferAccumulator.add(eventRecord);
            } catch (Exception ex) {
                LOG.error("Exception while adding record events {0}", ex);
            }
            if(Objects.nonNull(acknowledgementSet)){
                acknowledgementSet.add(eventRecord.getData());
            }
        });
        try {
            bufferAccumulator.flush();
        } catch (final Exception ex) {
            LOG.error("Exception while flushing record events to buffer {0}", ex);
        }
    }

    @Override
    public void run() {
        startProcessingCloudwatchMetrics(STANDARD_BACKOFF_MILLIS);
    }
}
