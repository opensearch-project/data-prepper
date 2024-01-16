/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import io.micrometer.core.instrument.Counter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.ToDoubleFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaTopicProducerMetricsTests {
    public static final class KafkaTestMetric implements Metric {
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

    private KafkaTopicProducerMetrics topicMetrics;

    private double bytesSent;
    private double recordsSent;
    private double bytesSendRate;
    private double recordSendRate;

    @Mock
    private Counter byteSendCounter;

    @Mock
    private Counter recordSendCounter;
    private double byteSendCount;
    private double recordSendCount;

    @BeforeEach
    void setUp() {
        topicName = RandomStringUtils.randomAlphabetic(8);
        bytesSent = 0.0;
        recordsSent = 0.0;
        bytesSendRate = 0.0;
        recordSendRate = 0.0;

        byteSendCount = 0.0;
        recordSendCount = 0.0;

        random = new Random();
        pluginMetrics = mock(PluginMetrics.class);
        pluginMetricsMap = new HashMap<>();
        doAnswer((i) -> {
            ToDoubleFunction f = i.getArgument(2);
            Object arg = i.getArgument(1);
            String name = i.getArgument(0);
            pluginMetricsMap.put(name, f);
            return f.applyAsDouble(arg);
        }).when(pluginMetrics).gauge(any(String.class), any(Object.class), any());
        byteSendCounter = mock(Counter.class);
        recordSendCounter = mock(Counter.class);

        doAnswer((i) -> {
            String arg = i.getArgument(0);
            if (arg.contains("Bytes")) {
                return byteSendCounter;
            } else {
                return recordSendCounter;
            }
        }).when(pluginMetrics).counter(any(String.class));
        doAnswer((i) -> {
            byteSendCount += (double)i.getArgument(0);
            return null;
        }).when(byteSendCounter).increment(any(Double.class));
        doAnswer((i) -> {
            recordSendCount += (double)i.getArgument(0);
            return null;
        }).when(recordSendCounter).increment(any(Double.class));
    }

    public KafkaTopicProducerMetrics createObjectUnderTest() {
        return new KafkaTopicProducerMetrics(topicName, pluginMetrics, true);
    }

    private KafkaTestMetric getMetric(final String name, final double value, Map<String, String> tags) {
        MetricName metricName = new MetricName(name, "group", "metric", tags);
        return new KafkaTestMetric(value, metricName);
    }
        

    private void populateKafkaMetrics(Map<MetricName, KafkaTestMetric> metrics, double numAssignedPartitions) {
        int tmpBytesProduced = random.nextInt() % 100 + 1;
        if (tmpBytesProduced < 0) {
            tmpBytesProduced = -tmpBytesProduced;
        }
        bytesSent += tmpBytesProduced;
        int tmpRecordsProduced = random.nextInt() % 10 + 1;
        if (tmpRecordsProduced < 0) {
            tmpRecordsProduced = -tmpRecordsProduced;
        }
        recordsSent += tmpRecordsProduced;

        double tmpBytesProducedRate = random.nextDouble()*100;
        bytesSendRate += tmpBytesProducedRate;

        double tmpRecordsProducedRate = random.nextDouble()*10;
        recordSendRate += tmpRecordsProducedRate;

        Map<String, Double> metricsMap = new HashMap<>();
        metricsMap.put("byte-total", (double)tmpBytesProduced);
        metricsMap.put("record-send-total", (double)tmpRecordsProduced);
        metricsMap.put("byte-rate", tmpBytesProducedRate);
        metricsMap.put("record-send-rate", tmpRecordsProducedRate);

        metricsMap.forEach((name, value) -> {
            Map<String, String> tags = Map.of("topic", topicName);
            KafkaTestMetric metric = getMetric(name, value, tags);
            metrics.put(metric.metricName(), metric);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10})
    public void KafkaTopicMetricTest_checkMetricUpdates(int numProducers) {
        topicMetrics = createObjectUnderTest();
        for (int i = 0; i < numProducers; i++) {
            KafkaProducer kafkaProducer = mock(KafkaProducer.class);
            topicMetrics.register(kafkaProducer);
            Map<MetricName, KafkaTestMetric> metrics = new HashMap<>();
            when(kafkaProducer.metrics()).thenReturn(metrics);
            populateKafkaMetrics(metrics, (i %2 == 1) ? 0.0 : 1.0);
            topicMetrics.update(kafkaProducer);
        }
        when(recordSendCounter.count()).thenReturn(recordSendCount);
        when(byteSendCounter.count()).thenReturn(byteSendCount);
        assertThat(topicMetrics.getNumberOfRecordsSent().count(), equalTo(recordsSent));
        assertThat(topicMetrics.getNumberOfBytesSent().count(), equalTo(bytesSent));
        pluginMetricsMap.forEach((k, v) -> {
            double result = v.applyAsDouble(topicMetrics.getMetricValues());
            if (k.contains("byteSendRate")) {
                assertEquals(result, bytesSendRate, 0.01d);
            } else if (k.contains("recordSendRate")) {
                assertEquals(result, recordSendRate, 0.01d);
            } else {
                assertThat(result, equalTo(k+": Unknown Metric"));
            }
        });
    }
}
