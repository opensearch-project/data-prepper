/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KafkaTopicProducerMetrics {
    static final String NUMBER_OF_RECORDS_SENT = "numberOfRecordsSent";
    static final String NUMBER_OF_BYTES_SENT = "numberOfBytesSent";
    static final String RECORD_SEND_TOTAL = "record-send-total";
    static final String BYTE_TOTAL = "byte-total";
    static final String TOPIC = "topic";
    static final String METRIC_SUFFIX_TOTAL = "-total";
    static final String BYTE_RATE = "byte-rate";
    static final String RECORD_SEND_RATE = "record-send-rate";
    static final String BYTE_SEND_RATE = "byteSendRate";
    static final String BYTE_SEND_TOTAL = "byteSendTotal";
    static final String RECORD_SEND_RATE_MAP_VALUE = "recordSendRate";
    static final String RECORD_SEND_TOTAL_MAP_VALUE = "recordSendTotal";
    static final String NUMBER_OF_RAW_DATA_SEND_ERRORS = "numberOfRawDataSendErrors";
    static final String NUMBER_OF_RECORD_SEND_ERRORS = "numberOfRecordSendErrors";
    static final String NUMBER_OF_RECORD_PROCESSING_ERRORS = "numberOfRecordProcessingErrors";
    static final String PRODUCE_DATA_PREPARATION_TIME = "produceDataPreparationTime";
    private final String topicName;
    private Map<String, String> metricsNameMap;
    private Map<KafkaProducer, Map<String, Double>> metricValues;
    private final PluginMetrics pluginMetrics;
    private final Counter numberOfRecordsSent;
    private final Counter numberOfBytesSent;
    private final Counter numberOfRawDataSendErrors;
    private final Counter numberOfRecordSendErrors;
    private final Counter numberOfRecordProcessingErrors;
    private final Timer produceDataPreparationTimer;

    public KafkaTopicProducerMetrics(final String topicName, final PluginMetrics pluginMetrics,
                                     final boolean topicNameInMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.topicName = topicName;
        this.metricValues = new HashMap<>();
        initializeMetricNamesMap(topicNameInMetrics);
        this.numberOfRecordsSent = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_RECORDS_SENT, topicNameInMetrics));
        this.numberOfBytesSent = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_BYTES_SENT, topicNameInMetrics));
        this.numberOfRawDataSendErrors = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_RAW_DATA_SEND_ERRORS, topicNameInMetrics));
        this.numberOfRecordSendErrors = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_RECORD_SEND_ERRORS, topicNameInMetrics));
        this.numberOfRecordProcessingErrors = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_RECORD_PROCESSING_ERRORS, topicNameInMetrics));
        this.produceDataPreparationTimer = pluginMetrics.timer(getTopicMetricName(PRODUCE_DATA_PREPARATION_TIME, topicNameInMetrics));
    }

    private void initializeMetricNamesMap(final boolean topicNameInMetrics) {
        this.metricsNameMap = new HashMap<>();
        metricsNameMap.put(BYTE_RATE, BYTE_SEND_RATE);
        metricsNameMap.put(BYTE_TOTAL, BYTE_SEND_TOTAL);
        metricsNameMap.put(RECORD_SEND_RATE, RECORD_SEND_RATE_MAP_VALUE);
        metricsNameMap.put(RECORD_SEND_TOTAL, RECORD_SEND_TOTAL_MAP_VALUE);

        metricsNameMap.forEach((metricName, camelCaseName) -> {
            if (!metricName.contains(METRIC_SUFFIX_TOTAL)) {
                pluginMetrics.gauge(getTopicMetricName(camelCaseName, topicNameInMetrics), metricValues, metricValues -> {
                    double sum = 0;
                    for (Map.Entry<KafkaProducer, Map<String, Double>> entry : metricValues.entrySet()) {
                        Map<String, Double> producerMetrics = entry.getValue();
                        synchronized(producerMetrics) {
                            sum += producerMetrics.get(metricName);
                        }
                    }
                    return sum;
                });
            }
        });
    }

    public void register(final KafkaProducer producer) {
        metricValues.put(producer, new HashMap<>());
        final Map<String, Double> producerMetrics = metricValues.get(producer);
        metricsNameMap.forEach((k, name) -> {
            producerMetrics.put(k, 0.0);
        });
    }

    Counter getNumberOfRecordsSent() {
        return numberOfRecordsSent;
    }

    Counter getNumberOfBytesSent() {
        return numberOfBytesSent;
    }

    public Counter getNumberOfRawDataSendErrors() {
        return numberOfRawDataSendErrors;
    }

    public Counter getNumberOfRecordSendErrors() {
        return numberOfRecordSendErrors;
    }

    public Counter getNumberOfRecordProcessingErrors() {
        return numberOfRecordProcessingErrors;
    }

    public Timer getProduceDataPreparationTimer() {
        return produceDataPreparationTimer;
    }

    private String getTopicMetricName(final String metricName, final boolean topicNameInMetrics) {
        if (topicNameInMetrics) {
            return "topic." + topicName + "." + metricName;
        } else {
            return metricName;
        }
    }

    Map<KafkaProducer, Map<String, Double>> getMetricValues() {
        return metricValues;
    }

    public void update(final KafkaProducer producer) {
        Map<String, Double> producerMetrics = metricValues.get(producer);

        Map<MetricName, ? extends Metric> metrics = producer.metrics();
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            MetricName metric = entry.getKey();
            Metric value = entry.getValue();
            String metricName = metric.name();
            if (Objects.nonNull(metricsNameMap.get(metricName))) {
                // producer metrics are emitted at topic level
                if (!metric.tags().containsKey(TOPIC)) {
                   continue;
                }

                double newValue = (Double)value.metricValue();
                if (metricName.equals(RECORD_SEND_TOTAL)) {
                    synchronized(producerMetrics) {
                        double prevValue = producerMetrics.get(metricName);
                        numberOfRecordsSent.increment(newValue - prevValue);
                    }
                } else if (metricName.equals(BYTE_TOTAL)) {
                    synchronized(producerMetrics) {
                        double prevValue = producerMetrics.get(metricName);
                        numberOfBytesSent.increment(newValue - prevValue);
                    }
                }

                synchronized(producerMetrics) {
                    producerMetrics.put(metricName, newValue);
                }
            }
        }
    }
}
