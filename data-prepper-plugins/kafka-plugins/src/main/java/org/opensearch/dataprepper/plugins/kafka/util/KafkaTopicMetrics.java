/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Instant;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;

public class KafkaTopicMetrics {
    private int metricUpdateInterval;
    private final String topicName;
    private long updateTime;
    private Map<KafkaConsumer, Map<String, Object>> consumerMetricsMap;
    private Map<String, String> camelCaseMap;
    private final PluginMetrics pluginMetrics;
    
    public KafkaTopicMetrics(final String topicName, final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.topicName = topicName;
        this.consumerMetricsMap = new HashMap<>();
        this.updateTime = Instant.now().getEpochSecond();
        this.metricUpdateInterval = 60; //seconds
        this.camelCaseMap = new HashMap<>();
        camelCaseMap.put("bytes-consumed-total", "bytesConsumedTotal");
        camelCaseMap.put("records-consumed-total", "recordsConsumedTotal");
        camelCaseMap.put("bytes-consumed-rate", "bytesConsumedRate");
        camelCaseMap.put("records-consumed-rate", "recordsConsumedRate");
        camelCaseMap.put("records-lag-max", "recordsLagMax");
        camelCaseMap.put("records-lead-min", "recordsLeadMin");
        camelCaseMap.put("commit-rate", "commitRate");
        camelCaseMap.put("join-rate", "joinRate");
        camelCaseMap.put("incoming-byte-rate", "incomingByteRate");
        camelCaseMap.put("outgoing-byte-rate", "outgoingByteRate");
        camelCaseMap.put("assigned-partitions", "outgoingByteRate");
    }

    public void register(KafkaConsumer consumer) {
        this.consumerMetricsMap.put(consumer, new HashMap<>());
    }

    private String getCamelCaseName(final String name) {
        String camelCaseName = camelCaseMap.get(name);
        if (Objects.isNull(camelCaseName)) {
            return name;
        }
        return camelCaseName;
    }

    public void update(final KafkaConsumer consumer, final String metricName, Integer metricValue) {
        synchronized(consumerMetricsMap) {
            Map<String, Object> cmetrics = consumerMetricsMap.get(consumer);
            if (cmetrics != null) {
                cmetrics.put(metricName, (double)metricValue);
            }
        }
    }

    public void update(final KafkaConsumer consumer) {
        Map<String, Object> cmetrics = null;
        synchronized(consumerMetricsMap) {
            cmetrics = consumerMetricsMap.get(consumer);
        }
        if (cmetrics == null) {
            return;
        }
        Map<MetricName, ? extends Metric> metrics = consumer.metrics();
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            MetricName metric = entry.getKey();
            Metric value = entry.getValue();
            String metricName = metric.name();
            String metricGroup = metric.group();
            if ((metricName.contains("consumed")) ||
                ((!metric.tags().containsKey("partition")) &&
                     (metricName.equals("records-lag-max") || metricName.equals("records-lead-min"))) ||
                (metricName.equals("commit-rate") || metricName.equals("join-rate")) ||
                (metricName.equals("incoming-byte-rate") || metricName.equals("outgoing-byte-rate"))) {
                    cmetrics.put(metricName, value.metricValue());
            }
        }
        synchronized (consumerMetricsMap) {
            long curTime = Instant.now().getEpochSecond();
            if (curTime - updateTime > metricUpdateInterval) {
                final Map<String, Double> aggregatedMetrics = new HashMap<>();
                consumerMetricsMap.forEach((c, metricsMap) -> {
                    Double value = 0.0;
                    metricsMap.forEach((metricName, metricValue) -> {
                        if (metricValue instanceof Double) {
                            aggregatedMetrics.put(metricName, ((Double)metricValue) + aggregatedMetrics.getOrDefault(metricName, 0.0));
                        }
                    });
                });
                aggregatedMetrics.forEach((name, value) -> {
                    System.out.println("__METRIC__topic."+topicName+"."+getCamelCaseName(name)+"___VALUE__"+value);
                    pluginMetrics.gauge("topic."+topicName+"."+getCamelCaseName(name), value);
                });
                updateTime = curTime;
            }
        }
    }
}
