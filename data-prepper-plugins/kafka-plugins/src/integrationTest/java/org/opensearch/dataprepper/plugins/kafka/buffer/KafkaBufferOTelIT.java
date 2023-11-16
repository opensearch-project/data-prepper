/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.model.buffer.DelegatingBuffer;
import org.opensearch.dataprepper.model.buffer.Buffer;


import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.opensearch.dataprepper.plugins.otel.codec.OTelMetricDecoder;
import org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsGrpcService;
import com.google.protobuf.util.JsonFormat;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaBufferOTelIT {
    private static final String TEST_REQUEST_MULTIPLE_METRICS_FILE = "test-request-multiple-metrics.json";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBufferIT.class);
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private KafkaBufferConfig kafkaBufferConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private BufferTopicConfig topicConfig;

    private DelegatingBuffer buffer;

    private PluginMetrics pluginMetrics;
    private String bootstrapServersCommaDelimited;
    private OTelMetricsGrpcService oTelMetricsGrpcService;
    class KafkaDelegatingBuffer extends DelegatingBuffer {
        KafkaDelegatingBuffer(Buffer buffer) {
            super(buffer);
        }
    };

    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(pluginSetting.getPipelineName()).thenReturn(UUID.randomUUID().toString());

        String topicName = "buffer-" + RandomStringUtils.randomAlphabetic(5);
        when(topicConfig.getName()).thenReturn(topicName);
        when(topicConfig.getGroupId()).thenReturn("buffergroup-" + RandomStringUtils.randomAlphabetic(6));
        when(topicConfig.isCreateTopic()).thenReturn(true);
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.BYTES);
        when(topicConfig.getWorkers()).thenReturn(1);
        when(topicConfig.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(topicConfig.getConsumerMaxPollRecords()).thenReturn(1);
        when(topicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(topicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(kafkaBufferConfig.getTopic()).thenReturn(topicConfig);

        EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);

        bootstrapServersCommaDelimited = System.getProperty("tests.kafka.bootstrap_servers");

        LOG.info("Using Kafka bootstrap servers: {}", bootstrapServersCommaDelimited);

        when(kafkaBufferConfig.getBootstrapServers()).thenReturn(Collections.singletonList(bootstrapServersCommaDelimited));
        when(kafkaBufferConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                KafkaBufferOTelIT.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }

    private ExportMetricsServiceRequest buildExportMetricsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportMetricsServiceRequest.Builder builder = ExportMetricsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    @Test
    void test_otel_metrics_with_kafka_buffer() throws Exception {
        KafkaBuffer kafkaBuffer = new KafkaBuffer(pluginSetting, kafkaBufferConfig, pluginFactory, acknowledgementSetManager, new OTelMetricDecoder(), null, null);
        buffer = new KafkaDelegatingBuffer(kafkaBuffer);
        oTelMetricsGrpcService = new OTelMetricsGrpcService(10000, 
            buffer,
            pluginMetrics);

        final ExportMetricsServiceRequest request = buildExportMetricsServiceRequestFromJsonFile(TEST_REQUEST_MULTIPLE_METRICS_FILE);
        oTelMetricsGrpcService.rawExport(request);
        Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = kafkaBuffer.read(10_000);
        assertThat(readResult, notNullValue());
        assertThat(readResult.getKey(), notNullValue());
        assertThat(readResult.getKey().size(), equalTo(3));
        for (Record<Event> record : readResult.getKey()) {
            Event event = record.getData();
            JacksonMetric metric = (JacksonMetric) (JacksonEvent)(Event)record.getData();
            if (metric.getKind().equals(Metric.KIND.GAUGE.toString())) {
                assertThat(metric.getUnit(), equalTo("1"));
                assertThat(metric.getName(), equalTo("counter-int"));
                JacksonGauge gauge = (JacksonGauge)metric;
                assertThat(gauge.getValue(), equalTo(123.0));
            } else if (metric.getKind().equals(Metric.KIND.SUM.toString())) {
                assertThat(metric.getUnit(), equalTo("1"));
                assertThat(metric.getName(), equalTo("sum-int"));
                JacksonSum sum = (JacksonSum)metric;
                assertThat(sum.getValue(), equalTo(456.0));
            } else if (metric.getKind().equals(Metric.KIND.HISTOGRAM.toString())) {
                assertThat(metric.getUnit(), equalTo("1"));
                assertThat(metric.getName(), equalTo("histogram-int"));
                JacksonHistogram histogram = (JacksonHistogram)metric;
                assertThat(histogram.getSum(), equalTo(100.0));
                assertThat(histogram.getCount(), equalTo(30L));
                assertThat(histogram.getExemplars(), equalTo(Collections.emptyList()));
                assertThat(histogram.getExplicitBoundsList(), equalTo(List.of(1.0, 2.0, 3.0, 4.0)));
                assertThat(histogram.getExplicitBoundsCount(), equalTo(4));
                assertThat(histogram.getBucketCountsList(), equalTo(List.of(3L, 5L, 15L, 6L, 1L)));
                assertThat(histogram.getBucketCount(), equalTo(5));
                assertThat(histogram.getAggregationTemporality(), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));
            } else {
                assertTrue("FAILED".equals("Unknown Metric type"));
            }    
        }
    }

}

