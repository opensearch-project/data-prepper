/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Metric;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.commons.lang3.RandomStringUtils;

import io.micrometer.core.instrument.Counter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.function.ToDoubleFunction; 

@ExtendWith(MockitoExtension.class)
public class KafkaTopicMetricsTests {
    public final class KafkaTestMetric implements Metric {
        private final Double value;
        private final MetricName name;

        public KafkaTestMetric(final double value, final MetricName name) {
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

    private String topicName;

    @Mock
    private PluginMetrics pluginMetrics;

    private Map<String, ToDoubleFunction> pluginMetricsMap;

    private Random random;

    private KafkaTopicMetrics topicMetrics;

    private double bytesConsumed;
    private double recordsConsumed;
    private double bytesConsumedRate;
    private double recordsConsumedRate;
    private double recordsLagMax;
    private double recordsLeadMin;
    private double commitRate;
    private double joinRate;
    private double incomingByteRate;
    private double outgoingByteRate;

    @Mock
    private Counter bytesConsumedCounter;

    @Mock
    private Counter recordsConsumedCounter;
    private double bytesConsumedCount;
    private double recordsConsumedCount;

    @BeforeEach
    void setUp() {
        topicName = RandomStringUtils.randomAlphabetic(8);
        bytesConsumed = 0.0;
        recordsConsumed = 0.0;
        bytesConsumedRate = 0.0;
        recordsConsumedRate = 0.0;
        recordsLagMax = 0.0;
        recordsLeadMin = Double.MAX_VALUE;
        commitRate = 0.0;
        joinRate = 0.0;
        incomingByteRate = 0.0;
        outgoingByteRate = 0.0;

        bytesConsumedCount = 0.0;
        recordsConsumedCount = 0.0;

        random = new Random();
        pluginMetrics = mock(PluginMetrics.class);
        pluginMetricsMap = new HashMap<>();
        doAnswer((i) -> {
            ToDoubleFunction f = (ToDoubleFunction)i.getArgument(2);
            Object arg = (Object)i.getArgument(1);
            String name = (String)i.getArgument(0);
            pluginMetricsMap.put(name, f);
            return f.applyAsDouble(arg);
        }).when(pluginMetrics).gauge(any(String.class), any(Object.class), any());
        bytesConsumedCounter = mock(Counter.class);
        recordsConsumedCounter = mock(Counter.class);

        doAnswer((i) -> {
            String arg = (String)i.getArgument(0);
            if (arg.contains("Bytes")) {
                return bytesConsumedCounter;
            } else {
                return recordsConsumedCounter;
            }
        }).when(pluginMetrics).counter(any(String.class));
        doAnswer((i) -> {
            bytesConsumedCount += (double)i.getArgument(0);
            return null;
        }).when(bytesConsumedCounter).increment(any(Double.class));
        doAnswer((i) -> {
            recordsConsumedCount += (double)i.getArgument(0);
            return null;
        }).when(recordsConsumedCounter).increment(any(Double.class));
    }

    public KafkaTopicMetrics createObjectUnderTest() {
        return new KafkaTopicMetrics(topicName, pluginMetrics);
    }

    private KafkaTestMetric getMetric(final String name, final double value, Map<String, String> tags) {
        MetricName metricName = new MetricName(name, "group", "metric", tags);
        return new KafkaTestMetric(value, metricName);
    }
        

    private void populateKafkaMetrics(Map<MetricName, KafkaTestMetric> metrics, double numAssignedPartitions) {
        Integer tmpBytesConsumed = random.nextInt() % 100 + 1;
        if (tmpBytesConsumed < 0) {
            tmpBytesConsumed = -tmpBytesConsumed;
        }
        bytesConsumed += tmpBytesConsumed;
        Integer tmpRecordsConsumed = random.nextInt() % 10 + 1;
        if (tmpRecordsConsumed < 0) {
            tmpRecordsConsumed = -tmpRecordsConsumed;
        }
        recordsConsumed += tmpRecordsConsumed;

        double tmpBytesConsumedRate = random.nextDouble()*100;
        bytesConsumedRate += tmpBytesConsumedRate;

        double tmpRecordsConsumedRate = random.nextDouble()*10;
        recordsConsumedRate += tmpRecordsConsumedRate;

        double tmpRecordsLagMax = random.nextDouble()*2;
        recordsLagMax = Math.max(recordsLagMax, tmpRecordsLagMax);

        double tmpRecordsLeadMin = random.nextDouble()*3;
        recordsLeadMin = Math.min(recordsLeadMin, tmpRecordsLeadMin);

        double tmpCommitRate = random.nextDouble();
        commitRate += tmpCommitRate;

        double tmpJoinRate = random.nextDouble();
        joinRate += tmpJoinRate;

        double tmpIncomingByteRate = random.nextDouble();
        incomingByteRate += tmpIncomingByteRate;

        double tmpOutgoingByteRate = random.nextDouble();
        outgoingByteRate += tmpOutgoingByteRate;

        Map<String, Double> metricsMap = new HashMap<>();
        metricsMap.put("bytes-consumed-total", (double)tmpBytesConsumed);
        metricsMap.put("records-consumed-total", (double)tmpRecordsConsumed);
        metricsMap.put("bytes-consumed-rate", tmpBytesConsumedRate);
        metricsMap.put("records-consumed-rate", tmpRecordsConsumedRate);
        metricsMap.put("records-lag-max", tmpRecordsLagMax);
        metricsMap.put("records-lead-min", tmpRecordsLeadMin);
        metricsMap.put("commit-rate", tmpCommitRate);
        metricsMap.put("join-rate", tmpJoinRate);
        metricsMap.put("incoming-byte-rate", tmpIncomingByteRate);
        metricsMap.put("outgoing-byte-rate", tmpOutgoingByteRate);
        metricsMap.put("assigned-partitions", numAssignedPartitions);

        metricsMap.forEach((name, value) -> {
            Map<String, String> tags = new HashMap<>();
            if (name.contains("-total")) {
                tags.put("topic", topicName);
            }
            KafkaTestMetric metric = getMetric(name, value, tags);
            metrics.put(metric.metricName(), metric);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10})
    //@ValueSource(ints = {2})
    public void KafkaTopicMetricTest_checkMetricUpdates(int numConsumers) {
        topicMetrics = createObjectUnderTest();
        for (int i = 0; i < numConsumers; i++) {
            KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);   
            topicMetrics.register(kafkaConsumer);
            Map<MetricName, KafkaTestMetric> metrics = new HashMap<>();
            when(kafkaConsumer.metrics()).thenReturn(metrics);
            populateKafkaMetrics(metrics, (i %2 == 1) ? 0.0 : 1.0);
            topicMetrics.update(kafkaConsumer);
        }
        when(recordsConsumedCounter.count()).thenReturn(recordsConsumedCount);
        when(bytesConsumedCounter.count()).thenReturn(bytesConsumedCount);
        assertThat(topicMetrics.getNumberOfRecordsConsumed().count(), equalTo(recordsConsumed));
        assertThat(topicMetrics.getNumberOfBytesConsumed().count(), equalTo(bytesConsumed));
        pluginMetricsMap.forEach((k, v) -> {
            double result = v.applyAsDouble(topicMetrics.getMetricValues());
            if (k.contains("bytesConsumedRate")) {
                assertEquals(result, bytesConsumedRate, 0.01d);
            } else if (k.contains("recordsConsumedRate")) {
                assertEquals(result, recordsConsumedRate, 0.01d);
            } else if (k.contains("recordsLagMax")) {
                assertEquals(result, recordsLagMax, 0.01d);
            } else if (k.contains("recordsLeadMin")) {
                assertEquals(result, recordsLeadMin, 0.01d);
            } else if (k.contains("commitRate")) {
                assertEquals(result, commitRate, 0.01d);
            } else if (k.contains("joinRate")) {
                assertEquals(result, joinRate, 0.01d);
            } else if (k.contains("incomingByteRate")) {
                assertEquals(result, incomingByteRate, 0.01d);
            } else if (k.contains("outgoingByteRate")) {
                assertEquals(result, outgoingByteRate, 0.01d);
            } else if (k.contains("numberOfNonConsumers")) {
                int expectedValue = numConsumers/2;
                assertThat(result, equalTo((double)expectedValue));
            } else {
                assertThat(result, equalTo(k+": Unknown Metric"));
            }
        });
        
    }

}
