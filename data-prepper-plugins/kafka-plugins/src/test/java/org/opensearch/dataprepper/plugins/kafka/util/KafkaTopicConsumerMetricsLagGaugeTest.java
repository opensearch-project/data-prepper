/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link KafkaTopicConsumerMetrics} emits an always-on {@code recordsLagPerPartition}
 * gauge tagged by partition, reflecting the consumer's per-partition {@code records-lag-max}.
 */
public class KafkaTopicConsumerMetricsLagGaugeTest {

    private static final class KafkaTestMetric implements Metric {
        private final Object value;
        private final MetricName name;

        private KafkaTestMetric(final Object value, final MetricName name) {
            this.value = value;
            this.name = name;
        }

        @Override
        public MetricName metricName() {
            return name;
        }

        @Override
        public Object metricValue() {
            return value;
        }
    }

    private SimpleMeterRegistry meterRegistry;
    private String topicName;

    @BeforeEach
    void setUp() {
        topicName = RandomStringUtils.randomAlphabetic(8);
        meterRegistry = new SimpleMeterRegistry();
        Metrics.addRegistry(meterRegistry);
    }

    @AfterEach
    void tearDown() {
        Metrics.removeRegistry(meterRegistry);
        meterRegistry.clear();
        meterRegistry.close();
    }

    private KafkaTopicConsumerMetrics newTopicMetrics() {
        return new KafkaTopicConsumerMetrics(topicName, PluginMetrics.fromNames("kafka", "test-pipeline"), true);
    }

    private MetricName lagMetricName(final String partition) {
        final Map<String, String> tags = new HashMap<>();
        tags.put("topic", topicName);
        tags.put("partition", partition);
        return new MetricName("records-lag-max", "consumer-fetch-manager-metrics", "per-partition lag", tags);
    }

    private void putLag(final Map<MetricName, KafkaTestMetric> metrics, final String partition, final double lag) {
        final MetricName name = lagMetricName(partition);
        metrics.put(name, new KafkaTestMetric(lag, name));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaConsumer consumerWithPartitionLag(final String partition, final double lag) {
        final KafkaConsumer consumer = mock(KafkaConsumer.class);
        final Map<MetricName, KafkaTestMetric> metrics = new HashMap<>();
        putLag(metrics, partition, lag);
        when(consumer.metrics()).thenReturn(metrics);
        return consumer;
    }

    private double lagGaugeValue(final String partition) {
        final String gaugeName = "test-pipeline.kafka.topic." + topicName + ".recordsLagPerPartition";
        return meterRegistry.find(gaugeName).tag("partition", partition).gauge().value();
    }

    @Test
    public void update_registersPerPartitionLagGaugeTaggedByPartition() {
        final KafkaTopicConsumerMetrics topicMetrics = newTopicMetrics();
        final KafkaConsumer consumer = consumerWithPartitionLag("3", 42.0);
        topicMetrics.register(consumer);

        topicMetrics.update(consumer);

        final String gaugeName = "test-pipeline.kafka.topic." + topicName + ".recordsLagPerPartition";
        assertThat("recordsLagPerPartition gauge for partition=3 should be registered",
                meterRegistry.find(gaugeName).tag("partition", "3").gauge(), notNullValue());
        assertThat(lagGaugeValue("3"), closeTo(42.0, 0.001));
    }

    @Test
    public void update_multiplePartitions_registersDistinctTaggedGauges() {
        final KafkaTopicConsumerMetrics topicMetrics = newTopicMetrics();
        final KafkaConsumer consumer = mock(KafkaConsumer.class);
        final Map<MetricName, KafkaTestMetric> metrics = new HashMap<>();
        putLag(metrics, "3", 10.0);
        putLag(metrics, "7", 99.0);
        when(consumer.metrics()).thenReturn(metrics);
        topicMetrics.register(consumer);

        topicMetrics.update(consumer);

        assertThat(lagGaugeValue("3"), closeTo(10.0, 0.001));
        assertThat(lagGaugeValue("7"), closeTo(99.0, 0.001));
    }

    @Test
    public void update_reportsLatestLagNotHighWaterMark() {
        final KafkaTopicConsumerMetrics topicMetrics = newTopicMetrics();
        final KafkaConsumer consumer = mock(KafkaConsumer.class);
        final Map<MetricName, KafkaTestMetric> metrics = new HashMap<>();
        when(consumer.metrics()).thenReturn(metrics);
        topicMetrics.register(consumer);

        putLag(metrics, "3", 500.0);
        topicMetrics.update(consumer);
        putLag(metrics, "3", 20.0);
        topicMetrics.update(consumer);

        // A live gauge must fall to the latest lag, not stay at the earlier high-water mark.
        assertThat(lagGaugeValue("3"), closeTo(20.0, 0.001));
    }

    @Test
    public void clearPartitionLag_zeroesTheGaugeForARevokedPartition() {
        final KafkaTopicConsumerMetrics topicMetrics = newTopicMetrics();
        final KafkaConsumer consumer = consumerWithPartitionLag("3", 42.0);
        topicMetrics.register(consumer);
        topicMetrics.update(consumer);

        topicMetrics.clearPartitionLag(3);

        assertThat(lagGaugeValue("3"), closeTo(0.0, 0.001));
    }
}
