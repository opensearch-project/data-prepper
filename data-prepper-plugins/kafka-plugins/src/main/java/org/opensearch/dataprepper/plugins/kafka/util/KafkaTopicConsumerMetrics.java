/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleUnaryOperator;

public class KafkaTopicConsumerMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicConsumerMetrics.class);
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
    static final String ACTUAL_POLL_INTERVAL = "actualPollInterval";
    static final String NUMBER_OF_ASSIGNED_PARTITIONS = "numberOfAssignedPartitions";
    static final String NUMBER_OF_ACTIVE_READERS = "numberOfActiveReaders";
    static final String NUMBER_OF_CONFIGURED_WORKERS = "numberOfConfiguredWorkers";
    static final String NUMBER_OF_REBALANCES = "numberOfRebalances";
    static final String NUMBER_OF_PARTITIONS_REVOKED = "numberOfPartitionsRevoked";
    static final String RECORDS_LAG_PER_PARTITION = "recordsLagPerPartition";
    static final String RECORDS_PROCESSING_LATENCY = "recordsProcessingLatency";
    // kafka-clients metric key; stored raw, inverted at read time (see update()).
    private static final String ASSIGNED_PARTITIONS = "assigned-partitions";

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
    private final Timer timeBetweenPollCalls;
    private Instant lastPollTime;

    private final Counter numberOfRebalances;
    private final Counter numberOfPartitionsRevoked;
    private final Timer recordsProcessingLatency;
    private final int configuredWorkers;

    private final Map<String, Double> perPartitionLag = new ConcurrentHashMap<>();
    private final Set<String> registeredLagGauges = ConcurrentHashMap.newKeySet();

    public KafkaTopicConsumerMetrics(final String topicName, final PluginMetrics pluginMetrics,
                                     final boolean topicNameInMetrics) {
        this(topicName, pluginMetrics, topicNameInMetrics, 0);
    }

    public KafkaTopicConsumerMetrics(final String topicName, final PluginMetrics pluginMetrics,
                                     final boolean topicNameInMetrics, final int configuredWorkers) {
        this.pluginMetrics = pluginMetrics;
        this.topicName = topicName;
        this.configuredWorkers = configuredWorkers;
        this.updateTime = Instant.now().getEpochSecond();
        // ConcurrentHashMap: mutated by every worker thread in update()/register() while the metric
        // scrape thread iterates it inside the gauge lambdas. A plain HashMap here is a
        // ConcurrentModificationException hazard, especially with the new scaling gauges below.
        this.metricValues = new ConcurrentHashMap<>();
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
        this.timeBetweenPollCalls = pluginMetrics.timer(getTopicMetricName(ACTUAL_POLL_INTERVAL, topicNameInMetrics));
        this.numberOfRebalances = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_REBALANCES, topicNameInMetrics));
        this.numberOfPartitionsRevoked = pluginMetrics.counter(getTopicMetricName(NUMBER_OF_PARTITIONS_REVOKED, topicNameInMetrics));
        this.recordsProcessingLatency = pluginMetrics.timer(getTopicMetricName(RECORDS_PROCESSING_LATENCY, topicNameInMetrics));
        registerScalingGauges(topicNameInMetrics);
        lastPollTime = Instant.now();
    }

    private void registerScalingGauges(final boolean topicNameInMetrics) {
        pluginMetrics.gauge(getTopicMetricName(NUMBER_OF_ASSIGNED_PARTITIONS, topicNameInMetrics), metricValues,
                mv -> foldAssignedPartitions(mv, raw -> raw));
        pluginMetrics.gauge(getTopicMetricName(NUMBER_OF_ACTIVE_READERS, topicNameInMetrics), metricValues,
                mv -> foldAssignedPartitions(mv, raw -> raw > 0 ? 1.0 : 0.0));
        // Register against the long-lived metricValues object, NOT gauge(name, Integer) whose weak ref
        // to the boxed Integer is GC'd to NaN.
        if (configuredWorkers > 0) {
            pluginMetrics.gauge(getTopicMetricName(NUMBER_OF_CONFIGURED_WORKERS, topicNameInMetrics), metricValues, mv -> (double) configuredWorkers);
        }
    }

    private static double foldAssignedPartitions(final Map<KafkaConsumer, Map<String, Double>> mv,
                                                 final DoubleUnaryOperator perConsumer) {
        double total = 0;
        for (final Map.Entry<KafkaConsumer, Map<String, Double>> entry : mv.entrySet()) {
            final Map<String, Double> consumerMetrics = entry.getValue();
            synchronized (consumerMetrics) {
                total += perConsumer.applyAsDouble(consumerMetrics.getOrDefault(ASSIGNED_PARTITIONS, 0.0));
            }
        }
        return total;
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
                        synchronized (consumerMetrics) {
                            if (consumerMetrics.get(metricName) == null) {
                                LOG.debug("No consumer metric for recordsLagMax found");
                            }
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
            } else if (metricName.equals("assigned-partitions")) {
                // numberOfNonConsumers = count of consumers with ZERO assigned partitions (read-time inversion).
                pluginMetrics.gauge(getTopicMetricName(camelCaseName, topicNameInMetrics), metricValues,
                        mv -> foldAssignedPartitions(mv, raw -> raw == 0.0 ? 1.0 : 0.0));
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

    public Counter getNumberOfRebalances() {
        return numberOfRebalances;
    }

    public Counter getNumberOfPartitionsRevoked() {
        return numberOfPartitionsRevoked;
    }

    public void recordProcessingLatency(final long durationMillis) {
        recordsProcessingLatency.record(durationMillis, TimeUnit.MILLISECONDS);
    }

    // Drop a closed consumer's entries so they stop skewing the aggregate gauges.
    public void deregister(final KafkaConsumer consumer) {
        metricValues.remove(consumer);
    }

    // Zero (not remove) a revoked partition's lag: the gauge meter stays registered, only the value resets.
    public void clearPartitionLag(final int partition) {
        perPartitionLag.put(String.valueOf(partition), 0.0);
    }

    public void recordTimeBetweenPolls() {
        final long timeBetweenPolls = Instant.now().toEpochMilli() - lastPollTime.toEpochMilli();
        timeBetweenPollCalls.record(timeBetweenPolls, TimeUnit.MILLISECONDS);
        lastPollTime = Instant.now();
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
                   if (metricName.equals("records-lag-max")) {
                       final String partition = metric.tags().get("partition");
                       final double lag = (Double) value.metricValue();
                       if (!Double.isNaN(lag) && !Double.isInfinite(lag)) {
                           // plain put (latest value), NOT merge(Math::max): a monotonic max would turn
                           // this into a high-water-mark rather than a live lag gauge.
                           perPartitionLag.put(partition, lag);
                           if (registeredLagGauges.add(partition)) {
                               // topic pinned in the name regardless of topicNameInMetrics: partition tags collide across topics otherwise.
                               pluginMetrics.gaugeWithTags(getTopicMetricName(RECORDS_LAG_PER_PARTITION, true),
                                       List.of(Tag.of("partition", partition)), perPartitionLag,
                                       m -> m.getOrDefault(partition, 0.0));
                           }
                       }
                   }
                   continue;
                }

                if (metricName.contains("consumed-total") && !metric.tags().containsKey("topic")) {
                    continue;
                }
                if (metricName.contains("byte-rate") && metric.tags().containsKey("node-id")) {
                    continue;
                }
                double newValue = (Double)value.metricValue();
                if (Double.isNaN(newValue) || Double.isInfinite(newValue)) {
                    LOG.debug("Skipping non-finite metric value {} for {}", newValue, metricName);
                    continue;
                }
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
                // Store the RAW assigned-partitions value; numberOfNonConsumers, numberOfAssignedPartitions
                // and numberOfActiveReaders all derive from it at read time in their gauge lambdas. This
                // replaced a destructive write-time inversion that overwrote the raw value.
                synchronized(consumerMetrics) {
                    consumerMetrics.put(metricName, newValue);
                }
            }
        }
    }
}
