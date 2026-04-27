/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.Map;

/**
 * Metrics for a Kafka producer. These span all topics, which makes it distinct from
 * the {@link KafkaTopicProducerMetrics}.
 */
public final class KafkaProducerMetrics {
    static final Map<String, String> METRICS_NAME_MAP = Map.of(
            "record-queue-time-avg", "recordQueueTimeAvg",
            "record-queue-time-max", "recordQueueTimeMax",
            "buffer-exhausted-rate", "bufferExhaustedRate",
            "buffer-available-bytes", "bufferAvailableBytes",
            "request-latency-avg", "requestLatencyAvg",
            "request-latency-max", "requestLatencyMax",
            "produce-throttle-time-avg", "produceThrottleTimeAvg",
            "produce-throttle-time-max", "produceThrottleTimeMax"
    );

    private KafkaProducerMetrics() { }

    public static void registerProducer(final PluginMetrics pluginMetrics, final KafkaProducer<?, ?> kafkaProducer) {
        final Map<MetricName, ? extends Metric> kafkaProducerMetrics = kafkaProducer.metrics();
        for (final Map.Entry<MetricName, ? extends Metric> metricNameEntry : kafkaProducerMetrics.entrySet()) {
            final MetricName kafkaMetricName = metricNameEntry.getKey();

            final String dataPrepperMetricName = METRICS_NAME_MAP.get(kafkaMetricName.name());
            if(dataPrepperMetricName == null)
                continue;

            final Metric metric = metricNameEntry.getValue();

            pluginMetrics.gauge(dataPrepperMetricName, metric, m -> (double) m.metricValue());
        }
    }
}
