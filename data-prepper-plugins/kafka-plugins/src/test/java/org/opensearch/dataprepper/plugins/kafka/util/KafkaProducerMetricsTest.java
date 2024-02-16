/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.ToDoubleFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerMetricsTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private KafkaProducer<?, ?> kafkaProducer;

    private Map<MetricName, Metric> kafkaMetricsMap;
    private Map<String, Metric> knownMetricsMap;

    @BeforeEach
    void setUp() {
        kafkaMetricsMap = new HashMap<>();
        knownMetricsMap = new HashMap<>();
        when(kafkaProducer.metrics()).thenReturn((Map) kafkaMetricsMap);

        KafkaProducerMetrics.METRICS_NAME_MAP.keySet()
                .stream()
                .map(KafkaProducerMetricsTest::createKafkaMetric)
                .forEach(metricName -> {
                    final Metric metric = mock(Metric.class);
                    knownMetricsMap.put(metricName.name(), metric);
                    kafkaMetricsMap.put(metricName, metric);
                });
        IntStream.range(0, 5)
                .mapToObj(ignored -> UUID.randomUUID().toString())
                .map(KafkaProducerMetricsTest::createKafkaMetric)
                .forEach(metricName -> kafkaMetricsMap.put(metricName, mock(Metric.class)));
    }

    @Test
    void registerProducer_creates_gauges_for_each_metric_from_the_map() {
        KafkaProducerMetrics.registerProducer(pluginMetrics, kafkaProducer);

        verify(pluginMetrics, times(KafkaProducerMetrics.METRICS_NAME_MAP.size())).gauge(anyString(), any(), any());
    }

    @ParameterizedTest
    @ArgumentsSource(RegisteredMetricsArgumentsProvider.class)
    void registerProduct_creates_expected_gauge(final String kafkaName, final String expectedDataPrepperName) {
        KafkaProducerMetrics.registerProducer(pluginMetrics, kafkaProducer);

        final Metric metric = knownMetricsMap.get(kafkaName);
        final ArgumentCaptor<ToDoubleFunction> metricFunctionArgumentCaptor = ArgumentCaptor.forClass(ToDoubleFunction.class);

        verify(pluginMetrics).gauge(eq(expectedDataPrepperName), eq(metric), metricFunctionArgumentCaptor.capture());

        final ToDoubleFunction actualMetricDoubleFunction = metricFunctionArgumentCaptor.getValue();

        final Random random = new Random();
        final double metricValue = random.nextDouble();
        when(metric.metricValue()).thenReturn(metricValue);
        assertThat(actualMetricDoubleFunction.applyAsDouble(metric), equalTo(metricValue));
    }

    private static MetricName createKafkaMetric(final String name) {
        final MetricName metricName = mock(MetricName.class);
        when(metricName.name()).thenReturn(name);
        return metricName;
    }

    static class RegisteredMetricsArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
            return KafkaProducerMetrics.METRICS_NAME_MAP.entrySet()
                    .stream()
                    .map(entry -> arguments(entry.getKey(), entry.getValue()));
        }
    }
}