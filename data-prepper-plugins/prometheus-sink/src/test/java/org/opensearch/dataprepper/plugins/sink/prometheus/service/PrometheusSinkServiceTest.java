/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import software.amazon.awssdk.utils.Pair;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class PrometheusSinkServiceTest {

    private static final String TEST_PIPELINE_NAME = "testPipeline";
    private static final String TEST_PLUGIN_NAME = "testPipeline";
    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));
        

    private static final String SINK_YAML =
            "        url: \"http://localhost:8080/test\"\n" +
            "        threshold:\n"+
            "          max_events: 2\n" +
            "          flush_interval: 10\n"+
            "        connection_timeout: 10\n"+
            "        idle_timeout: 10\n"+
            "        aws:\n" +
            "          region: \"us-east-2\"\n" +
            "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
            "          sts_external_id: \"test-external-id\"\n" +
            "          sts_header_overrides: {\"test\": test }\n" +
            "        max_retries: 5\n" +
            "        encoding: snappy\n" +
            "        content_type: \"application/x-protobuf\"\n" +
            "        remote_write_version: 0.1.0\n";

    private PrometheusSinkConfiguration prometheusSinkConfiguration;

    private HeadlessPipeline dlqPipeline;

    private PluginSetting pluginSetting;

    private PluginMetrics pluginMetrics;
    
    private SinkMetrics sinkMetrics;

    private PrometheusHttpSender httpSender;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private Counter prometheusSinkRecordsSuccessCounter;

    private Counter prometheusSinkRecordsFailedCounter;

    private EventHandle eventHandle;

    @BeforeEach
    void setup() throws IOException {
        objectMapper.registerModule(new JavaTimeModule());
        this.pluginMetrics = mock(PluginMetrics.class);
        this.dlqPipeline = mock(HeadlessPipeline.class);
        this.httpSender = mock(PrometheusHttpSender.class);
        this.sinkMetrics = mock(SinkMetrics.class);
        eventHandle = mock(EventHandle.class);
        this.prometheusSinkConfiguration = objectMapper.readValue(SINK_YAML,PrometheusSinkConfiguration.class);
        this.pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        when(pluginSetting.getName()).thenReturn(TEST_PLUGIN_NAME);
        this.awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        this.prometheusSinkRecordsSuccessCounter = mock(Counter.class);
        this.prometheusSinkRecordsFailedCounter = mock(Counter.class);
        when(pluginMetrics.counter(PrometheusSinkService.PROMETHEUS_SINK_RECORDS_SUCCESS_COUNTER)).thenReturn(prometheusSinkRecordsSuccessCounter);
        when(pluginMetrics.counter(PrometheusSinkService.PROMETHEUS_SINK_RECORDS_FAILED_COUNTER)).thenReturn(prometheusSinkRecordsFailedCounter);

    }

    PrometheusSinkService createObjectUnderTest(final PrometheusSinkConfiguration prometheusSinkConfig, final HeadlessPipeline dlqPipeline) {
        return new PrometheusSinkService(
                prometheusSinkConfig,
                sinkMetrics,
                httpSender,
                dlqPipeline,
                pluginMetrics,
                pluginSetting);
    }

    @Test
    void prometheusSinkServiceTestSuccessfulOutput() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndPoint(any())).thenReturn(Pair.of(true, 0));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration, null);
        JacksonGauge gauge1 = createGaugeMetric("gauge1");
        JacksonGauge gauge2 = createGaugeMetric("gauge2");
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsSuccessCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsSuccessCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(true));
        
    }

    @Test
    void prometheusSinkServiceTestFailedOutput() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndPoint(any())).thenReturn(Pair.of(false, 410));
        Pipeline dlqPipeline = mock(Pipeline.class);
        doAnswer(a -> {
            Collection<Record<Event>> records = (Collection<Record<Event>>)a.getArgument(0);
            for (final Record<Event> record : records) {
                Event event = record.getData();
                assertThat(event.get("_failure_metadata/statusCode", Integer.class), equalTo(410));
                assertThat(event.get("_failure_metadata/pluginName", String.class), equalTo(TEST_PLUGIN_NAME));
                assertThat(event.get("_failure_metadata/pipelineName", String.class), equalTo(TEST_PIPELINE_NAME));
                event.getEventHandle().release(true);
            }
            return null;
        }).when(dlqPipeline).sendEvents(any(Collection.class));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration, null);
        objectUnderTest.setDlqPipeline(dlqPipeline);
        JacksonGauge gauge1 = createGaugeMetric("gauge1");
        JacksonGauge gauge2 = createGaugeMetric("gauge2");
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsFailedCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsFailedCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(true));
        
    }

    @Test
    void prometheusSinkServiceTestFailedOutputWithNoDLQ() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndPoint(any())).thenReturn(Pair.of(false, 410));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration, null);
        JacksonGauge gauge1 = createGaugeMetric("gauge1");
        JacksonGauge gauge2 = createGaugeMetric("gauge2");
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsFailedCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsFailedCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(false));
    }

    @Test
    void prometheusSinkServiceTestWithExceptionInHttpSender() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndPoint(any())).thenThrow(new RuntimeException("exception"));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration, null);
        JacksonGauge gauge1 = createGaugeMetric("gauge1");
        JacksonGauge gauge2 = createGaugeMetric("gauge2");
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsFailedCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsFailedCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(false));
    }



    @Test
    void prometheus_sink_service_test_output_with_zero_record() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration, null);
        Collection<Record<Event>> records = List.of();
        objectUnderTest.output(records);
    }

    private JacksonGauge createGaugeMetric(final String name) {
        return JacksonGauge.builder()
            .withName(name)
            .withDescription("Test Gauge Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withUnit("1")
            .withValue(1.0d)
            .withEventHandle(eventHandle)
            .build(false);
    }

}
