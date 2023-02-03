/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.metricpublisher;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MicrometerMetricPublisher implements MetricPublisher {
    private static final String SERVICE_ID = "ServiceId";
    private static final String OPERATION_NAME = "OperationName";
    private static final Set<SdkMetric<String>> DEFAULT_DIMENSIONS = Stream.of(CoreMetric.SERVICE_ID, CoreMetric.OPERATION_NAME)
            .collect(Collectors.toSet());

    private final PluginMetrics pluginMetrics;

    public MicrometerMetricPublisher(final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public void publish(final MetricCollection metricCollection) {
        final Map<String, String> dimensions = metricCollection.stream()
                .filter(metricRecord -> DEFAULT_DIMENSIONS.contains(metricRecord.metric()))
                .collect(Collectors.toMap(metricRecord -> metricRecord.metric().name(), metricRecord -> metricRecord.value().toString()));

        addMetricsToPluginMetrics(metricCollection, dimensions);
        metricCollection.children().forEach(
                child -> addMetricsToPluginMetrics(child, dimensions)
        );
    }

    @Override
    public void close() {

    }

    private void addMetricsToPluginMetrics(final MetricCollection metricCollection, final Map<String, String> dimensions) {
        final String serviceIdValue = dimensions.get(SERVICE_ID);
        final String operationNameValue = dimensions.get(OPERATION_NAME);

        metricCollection.forEach(metricRecord -> {
            final Class<?> metricValueClass = metricRecord.metric().valueClass();
            final String metricName = metricRecord.metric().name();

            if (Duration.class.isAssignableFrom(metricValueClass)) {
                final long metricValueInMillis = Duration.parse(metricRecord.value().toString()).toMillis();
                pluginMetrics.timerWithTags(metricName, SERVICE_ID, serviceIdValue, OPERATION_NAME, operationNameValue)
                        .record(metricValueInMillis, TimeUnit.MILLISECONDS);
            } else if (Number.class.isAssignableFrom(metricValueClass)) {
                final Number numberMetricValue = (Number) metricRecord.value();
                pluginMetrics.counterWithTags(metricName, SERVICE_ID, serviceIdValue, OPERATION_NAME, operationNameValue)
                        .increment(numberMetricValue.doubleValue());
            } else if (Boolean.class.isAssignableFrom(metricValueClass)) {
                final Boolean booleanValue = (Boolean) metricRecord.value();
                final double metricValue = Boolean.TRUE.equals(booleanValue) ? 1.0 : 0.0;
                pluginMetrics.counterWithTags(metricName, SERVICE_ID, serviceIdValue, OPERATION_NAME, operationNameValue)
                        .increment(metricValue);
            }
        });
    }
}
