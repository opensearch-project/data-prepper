/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Instant;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;

public class KafkaTopicConsumerMetrics {
    static final String NUMBER_OF_POSITIVE_ACKNOWLEDGEMENTS = "numberOfPositiveAcknowledgements";
    static final String NUMBER_OF_NEGATIVE_ACKNOWLEDGEMENTS = "numberOfNegativeAcknowledgements";
    static final String NUMBER_OF_RECORDS_FAILED_TO_PARSE = "numberOfRecordsFailedToParse";
    static final String NUMBER_OF_DESERIALIZATION_ERRORS = "numberOfDeserializationErrors";
    static final String NUMBER_OF_BUFFER_SIZE_OVERFLOWS = "numberOfBufferSizeOverflows";
    static final String NUMBER_OF_INVALID_TIMESTAMPS = "numberOfInvalidTimeStamps";
    static final String NUMBER_OF_POLL_AUTH_ERRORS = "numberOfPollAuthErrors";
    static final String NUMBER_OF_RECORDS_COMMITTED = "numberOfRecordsCommitted";
    static final String NUMBER_OF_RECORDS_CONSUMED = "numberOfRecordsConsumed";
    static final String NUMBER_OF_BYTES_CONSUMED = "numberOfBytesConsumed";

    private final String topicName;
    private long updateTime;
    private Map<String, String> metricsNameMap;
    private Map<KafkaConsumer, Map<String, Double>> metricValues;
    private final PluginMetrics pluginMetrics;
    private final Counter numberOfPositiveAcknowledgements;
    private final Counter numberOfNegativeAcknowledgements;
    private final Counter numberOfRecordsFailedToParse;
    private final Counter numberOfDeserializationErrors;
    private final Counter numberOfBufferSizeOverflows;
    private final Counter numberOfPollAuthErrors;
    private final Counter numberOfInvalidTimeStamps;
    private final Counter numberOfRecordsCommitted;
    private final Counter numberOfRecordsConsumed;
    private final Counter numberOfBytesConsumed;

    public KafkaTopicConsumerMetrics(final String topicName, final PluginMetrics pluginMetrics,
                                     final boolean topicNameInMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.topicName = topicName;
        this.updateTime = Instant.now().getEpochSecond();
        this.metricValues = new HashMap<>();
        initializeMetricNamesMap(topicNameInMetrics);
        this.numberOfRecordsConsumed = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_RECORDS_CONSUMED, topicNameInMetrics));
        this.numberOfBytesConsumed = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_BYTES_CONSUMED, topicNameInMetrics));
        this.numberOfRecordsCommitted = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_RECORDS_COMMITTED, topicNameInMetrics));
        this.numberOfRecordsFailedToParse = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_RECORDS_FAILED_TO_PARSE, topicNameInMetrics));
        this.numberOfInvalidTimeStamps = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_INVALID_TIMESTAMPS, topicNameInMetrics));
        this.numberOfDeserializationErrors = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_DESERIALIZATION_ERRORS, topicNameInMetrics));
        this.numberOfBufferSizeOverflows = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_BUFFER_SIZE_OVERFLOWS, topicNameInMetrics));
        this.numberOfPollAuthErrors = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_POLL_AUTH_ERRORS, topicNameInMetrics));
        this.numberOfPositiveAcknowledgements = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_POSITIVE_ACKNOWLEDGEMENTS, topicNameInMetrics));
        this.numberOfNegativeAcknowledgements = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_NEGATIVE_ACKNOWLEDGEMENTS, topicNameInMetrics));
    }

    private void initializeMetricNamesMap(final boolean topicNameInMetrics) {
        this.metricsNameMap = new HashMap<>();
        this.metricsNameMap.put("bytes-consumed-total", "bytesConsumedTotal");
        this.metricsNameMap.put("records-consumed-total", "recordsConsumedTotal");
        this.metricsNameMap.put("bytes-consumed-rate", "bytesConsumedRate");
        this.metricsNameMap.put("records-consumed-rate", "recordsConsumedRate");
        this.metricsNameMap.put("records-lag-max", "recordsLagMax");
        this.metricsNameMap.put("records-lead-min", "recordsLeadMin");
        this.metricsNameMap.put("commit-rate", "commitRate");
        this.metricsNameMap.put("join-rate", "joinRate");
        this.metricsNameMap.put("incoming-byte-rate", "incomingByteRate");
        this.metricsNameMap.put("outgoing-byte-rate", "outgoingByteRate");
        this.metricsNameMap.put("assigned-partitions", "numberOfNonConsumers");
        this.metricsNameMap.forEach((metricName, camelCaseName) -> {
            if (metricName.equals("records-lag-max")) {
                pluginMetrics.gauge(getTopicMetricName(camelCaseName, topicNameInMetrics), metricValues, metricValues -> {
                    double max = 0.0;
                    for (Map.Entry<KafkaConsumer, Map<String, Double>> entry : metricValues.entrySet()) {
                        Map<String, Double> consumerMetrics = entry.getValue();
                        synchronized(consumerMetrics) {
                            max = Math.max(max, consumerMetrics.get(metricName));
                        }
                    }
                    return max;
                });
            } else if (metricName.equals("records-lead-min")) {
                pluginMetrics.gauge(getTopicMetricName(camelCaseName, topicNameInMetrics), metricValues, metricValues -> {
                    double min = Double.MAX_VALUE;
                    for (Map.Entry<KafkaConsumer, Map<String, Double>> entry : metricValues.entrySet()) {
                        Map<String, Double> consumerMetrics = entry.getValue();
                        synchronized(consumerMetrics) {
                            min = Math.min(min, consumerMetrics.get(metricName));
                        }
                    }
                    return min;
                });
            } else if (!metricName.contains("-total")) {
                pluginMetrics.gauge(getTopicMetricName(camelCaseName, topicNameInMetrics), metricValues, metricValues -> {
                    double sum = 0;
                    for (Map.Entry<KafkaConsumer, Map<String, Double>> entry : metricValues.entrySet()) {
                        Map<String, Double> consumerMetrics = entry.getValue();
                        synchronized(consumerMetrics) {
                            sum += consumerMetrics.get(metricName);
                        }
                    }
                    return sum;
                });
            }
        });
    }

    public void register(final KafkaConsumer consumer) {
        metricValues.put(consumer, new HashMap<>());
        final Map<String, Double> consumerMetrics = metricValues.get(consumer);
        metricsNameMap.forEach((k, name) -> {
            consumerMetrics.put(k, 0.0);
        });
    }

    Counter getNumberOfRecordsConsumed() {
        return numberOfRecordsConsumed;
    }

    Counter getNumberOfBytesConsumed() {
        return numberOfBytesConsumed;
    }

    public Counter getNumberOfRecordsCommitted() {
        return numberOfRecordsCommitted;
    }

    public Counter getNumberOfPollAuthErrors() {
        return numberOfPollAuthErrors;
    }

    public Counter getNumberOfBufferSizeOverflows() {
        return numberOfBufferSizeOverflows;
    }

    public Counter getNumberOfDeserializationErrors() {
        return numberOfDeserializationErrors;
    }

    public Counter getNumberOfRecordsFailedToParse() {
        return numberOfRecordsFailedToParse;
    }

    public Counter getNumberOfNegativeAcknowledgements() {
        return numberOfNegativeAcknowledgements;
    }

    public Counter getNumberOfInvalidTimeStamps() {
        return numberOfInvalidTimeStamps;
    }

    public Counter getNumberOfPositiveAcknowledgements() {
        return numberOfPositiveAcknowledgements;
    }

    private String getTopicMetricName(final String metricName, final boolean topicNameInMetrics) {
        if (topicNameInMetrics) {
            return "topic." + topicName + "." + metricName;
        } else {
            return metricName;
        }
    }

    private String getCamelCaseName(final String name) {
        String camelCaseName = metricsNameMap.get(name);
        if (Objects.isNull(camelCaseName)) {
            return name;
        }
        return camelCaseName;
    }

    Map<KafkaConsumer, Map<String, Double>> getMetricValues() {
        return metricValues;
    }

    public void update(final KafkaConsumer consumer) {
        Map<String, Double> consumerMetrics = metricValues.get(consumer);
        Map<MetricName, ? extends Metric> metrics = consumer.metrics();
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            MetricName metric = entry.getKey();
            Metric value = entry.getValue();
            String metricName = metric.name();
            if (Objects.nonNull(metricsNameMap.get(metricName))) {
                if (metric.tags().containsKey("partition") &&
                   (metricName.equals("records-lag-max") || metricName.equals("records-lead-min"))) {
                   continue;
                }

                if (metricName.contains("consumed-total") && !metric.tags().containsKey("topic")) {
                    continue;
                }
                if (metricName.contains("byte-rate") && metric.tags().containsKey("node-id")) {
                    continue;
                }
                double newValue = (Double)value.metricValue();
                if (metricName.equals("records-consumed-total")) {
                    synchronized(consumerMetrics) {
                        double prevValue = consumerMetrics.get(metricName);
                        numberOfRecordsConsumed.increment(newValue - prevValue);
                    }
                } else if (metricName.equals("bytes-consumed-total")) {
                    synchronized(consumerMetrics) {
                        double prevValue = consumerMetrics.get(metricName);
                        numberOfBytesConsumed.increment(newValue - prevValue);
                    }
                }
                // Keep the count of number of consumers without any assigned partitions. This value can go up or down. So, it is made as Guage metric
                if (metricName.equals("assigned-partitions")) {
                    newValue = (newValue == 0.0) ? 1.0 : 0.0;
                }
                synchronized(consumerMetrics) {
                    consumerMetrics.put(metricName, newValue);
                }
            }
        }
    }
}
