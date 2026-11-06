 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.arpnetworking.metrics.prometheus.Types.Label;
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
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.host.HostContext;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusPushResult;
import org.opensearch.dataprepper.common.sink.SinkMetrics;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class PrometheusSinkServiceTest {

    private static final String TEST_PIPELINE_NAME = "testPipeline";
    private static final String HOST_ID_A = "3f2b1c4d5e6f7a8b";
    private static final String HOST_ID_B = "9c8d7e6f5a4b3c2d";
    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private static final String SINK_YAML =
            "        url: \"http://localhost:8080/test\"\n" +
            "        threshold:\n"+
            "          max_events: 2\n" +
            "          flush_interval: 10\n"+
            "        connection_timeout: 10\n"+
            "        out_of_order_time_window: 0\n" +
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

    private PipelineDescription pipelineDescription;

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
        this.pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        this.awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        this.prometheusSinkRecordsSuccessCounter = mock(Counter.class);
        this.prometheusSinkRecordsFailedCounter = mock(Counter.class);
        when(pluginMetrics.counter(PrometheusSinkService.PROMETHEUS_SINK_RECORDS_SUCCESS_COUNTER)).thenReturn(prometheusSinkRecordsSuccessCounter);
        when(pluginMetrics.counter(PrometheusSinkService.PROMETHEUS_SINK_RECORDS_FAILED_COUNTER)).thenReturn(prometheusSinkRecordsFailedCounter);

    }

    PrometheusSinkService createObjectUnderTest(final PrometheusSinkConfiguration prometheusSinkConfig) {
        return new PrometheusSinkService(
                prometheusSinkConfig,
                sinkMetrics,
                httpSender,
                pipelineDescription);
    }

    @Test
    void prometheusSinkServiceTestSuccessfulOutput() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndpoint(any())).thenReturn(new PrometheusPushResult(true, 0));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration);
        JacksonGauge gauge1 = createGaugeMetric("gauge1", null);
        JacksonGauge gauge2 = createGaugeMetric("gauge2", null);
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsSuccessCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsSuccessCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(true));
    }

    @Test
    void prometheusSinkServiceTestSuccessfulOutputWithWindow1() throws NoSuchFieldException, IllegalAccessException, IOException {
        String newYaml = SINK_YAML.replace("out_of_order_window: 0", "out_of_order_window: 1");
        this.prometheusSinkConfiguration = objectMapper.readValue(newYaml,PrometheusSinkConfiguration.class);
        when(httpSender.pushToEndpoint(any())).thenReturn(new PrometheusPushResult(true, 0));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration);
        Instant t = Instant.now();
        JacksonGauge gauge1 = createGaugeMetric("gauge1", t);
        JacksonGauge gauge2 = createGaugeMetric("gauge1", t.plusMillis(100));
        JacksonGauge gauge3 = createGaugeMetric("gauge1", t.plusSeconds(20));
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2), new Record<>(gauge3));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsSuccessCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsSuccessCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(true));
    }

    @Test
    void prometheusSinkServiceTestFailedOutput() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndpoint(any())).thenReturn(new PrometheusPushResult(false, 410));
        Pipeline dlqPipeline = mock(Pipeline.class);
        doAnswer(a -> {
            Collection<Record<Event>> records = (Collection<Record<Event>>)a.getArgument(0);
            for (final Record<Event> record : records) {
                Event event = record.getData();
                assertThat(event.get("_failure_metadata/statusCode", Integer.class), equalTo(410));
                assertThat(event.get("_failure_metadata/pluginName", String.class), equalTo(PrometheusSinkService.PLUGIN_NAME));
                assertThat(event.get("_failure_metadata/pipelineName", String.class), equalTo(TEST_PIPELINE_NAME));
                event.getEventHandle().release(true);
            }
            return null;
        }).when(dlqPipeline).sendEvents(any(Collection.class));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration);
        objectUnderTest.setDlqPipeline(dlqPipeline);
        JacksonGauge gauge1 = createGaugeMetric("gauge1", null);
        JacksonGauge gauge2 = createGaugeMetric("gauge2", null);
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsFailedCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsFailedCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(true));
    }

    @Test
    void prometheusSinkServiceTestFailedOutputWithNoDLQ() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndpoint(any())).thenReturn(new PrometheusPushResult(false, 410));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration);
        JacksonGauge gauge1 = createGaugeMetric("gauge1", null);
        JacksonGauge gauge2 = createGaugeMetric("gauge2", null);
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsFailedCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsFailedCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(false));
    }

    @Test
    void prometheusSinkServiceTestWithExceptionInHttpSender() throws NoSuchFieldException, IllegalAccessException {
        when(httpSender.pushToEndpoint(any())).thenThrow(new RuntimeException("exception"));
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration);
        JacksonGauge gauge1 = createGaugeMetric("gauge1", null);
        JacksonGauge gauge2 = createGaugeMetric("gauge2", null);
        Collection<Record<Event>> records = List.of(new Record<>(gauge1), new Record<>(gauge2));
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
        verify(sinkMetrics, times(1)).incrementRequestsFailedCounter(any(Integer.class));
        verify(sinkMetrics, times(1)).incrementEventsFailedCounter(any(Integer.class));
        verify(eventHandle, times(2)).release(eq(false));
    }



    @Test
    void prometheus_sink_service_test_output_with_zero_record() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(prometheusSinkConfiguration);
        Collection<Record<Event>> records = List.of();
        objectUnderTest.output(records);
    }

    @Test
    void instance_label_is_added_to_every_series_with_the_resolved_host_id() throws Exception {
        final PrometheusSinkConfiguration config = objectMapper.readValue(
                "        url: \"http://localhost:9090/api/v1/write\"\n" +
                        "        insecure: true\n" +
                        "        instance_label: dp_instance\n", PrometheusSinkConfiguration.class);
        final PrometheusSinkService objectUnderTest = new PrometheusSinkService(
                config, sinkMetrics, httpSender, pipelineDescription, () -> HOST_ID_A);

        final PrometheusSinkBufferEntry entry =
                (PrometheusSinkBufferEntry) objectUnderTest.getSinkBufferEntry(createGaugeMetric("gauge1", null));

        assertThat(labelsOf(entry).get("dp_instance"), equalTo(HOST_ID_A));
    }

    @Test
    void the_host_id_is_resolved_once_rather_than_per_event() throws Exception {
        final PrometheusSinkConfiguration config = objectMapper.readValue(
                "        url: \"http://localhost:9090/api/v1/write\"\n" +
                        "        insecure: true\n" +
                        "        instance_label: dp_instance\n", PrometheusSinkConfiguration.class);
        final AtomicInteger resolutions = new AtomicInteger();
        final PrometheusSinkService objectUnderTest = new PrometheusSinkService(
                config, sinkMetrics, httpSender, pipelineDescription,
                () -> {
                    resolutions.incrementAndGet();
                    return HOST_ID_A;
                });

        objectUnderTest.getSinkBufferEntry(createGaugeMetric("gauge1", null));
        objectUnderTest.getSinkBufferEntry(createGaugeMetric("gauge2", null));

        assertThat(resolutions.get(), equalTo(1));
    }

    @Test
    void the_host_id_is_not_resolved_when_no_instance_label_is_configured() throws Exception {
        final PrometheusSinkConfiguration config = objectMapper.readValue(
                "        url: \"http://localhost:9090/api/v1/write\"\n" +
                        "        insecure: true\n", PrometheusSinkConfiguration.class);
        final AtomicInteger resolutions = new AtomicInteger();

        new PrometheusSinkService(config, sinkMetrics, httpSender, pipelineDescription,
                () -> {
                    resolutions.incrementAndGet();
                    return HOST_ID_A;
                });

        assertThat(resolutions.get(), equalTo(0));
    }

    @Test
    void no_instance_label_is_added_when_it_is_not_configured() throws Exception {
        final PrometheusSinkConfiguration config = objectMapper.readValue(
                "        url: \"http://localhost:9090/api/v1/write\"\n" +
                        "        insecure: true\n", PrometheusSinkConfiguration.class);
        final PrometheusSinkService objectUnderTest = new PrometheusSinkService(
                config, sinkMetrics, httpSender, pipelineDescription, () -> HOST_ID_A);

        final PrometheusSinkBufferEntry entry =
                (PrometheusSinkBufferEntry) objectUnderTest.getSinkBufferEntry(createGaugeMetric("gauge1", null));

        assertThat(labelsOf(entry).containsKey("dp_instance"), equalTo(false));
    }

    @Test
    void the_instance_label_value_is_the_stable_host_id() {
        try (MockedStatic<HostContext> hostContext = mockStatic(HostContext.class)) {
            hostContext.when(HostContext::isHostIdentityResolved).thenReturn(true);
            hostContext.when(HostContext::getStableHostId).thenReturn(HOST_ID_A);

            assertThat(PrometheusSinkService.resolveInstanceId(), equalTo(HOST_ID_A));
        }
    }

    @Test
    void the_pipeline_fails_to_start_when_no_host_identity_can_be_resolved() {
        try (MockedStatic<HostContext> hostContext = mockStatic(HostContext.class)) {
            hostContext.when(HostContext::isHostIdentityResolved).thenReturn(false);

            final InvalidPluginConfigurationException exception = assertThrows(
                    InvalidPluginConfigurationException.class, PrometheusSinkService::resolveInstanceId);

            assertThat(exception.getMessage(), containsString("instance_label"));
            assertThat(exception.getMessage(), containsString("HOSTNAME"));
        }
    }

    @Test
    void two_instances_sharing_one_configuration_produce_different_series() throws Exception {
        final String yaml = "        url: \"http://localhost:9090/api/v1/write\"\n" +
                "        insecure: true\n" +
                "        instance_label: dp_instance\n";
        final PrometheusSinkConfiguration sharedConfig = objectMapper.readValue(yaml, PrometheusSinkConfiguration.class);

        final PrometheusSinkService instanceA = new PrometheusSinkService(
                sharedConfig, sinkMetrics, httpSender, pipelineDescription, () -> HOST_ID_A);
        final PrometheusSinkService instanceB = new PrometheusSinkService(
                sharedConfig, sinkMetrics, httpSender, pipelineDescription, () -> HOST_ID_B);

        final String seriesA = labelsOf((PrometheusSinkBufferEntry)
                instanceA.getSinkBufferEntry(createGaugeMetric("gauge1", null))).toString();
        final String seriesB = labelsOf((PrometheusSinkBufferEntry)
                instanceB.getSinkBufferEntry(createGaugeMetric("gauge1", null))).toString();

        assertThat(seriesA.equals(seriesB), equalTo(false));
    }

    private Map<String, String> labelsOf(final PrometheusSinkBufferEntry entry) {
        return entry.getTimeSeries().getTimeSeriesList().get(0).getLabelsList().stream()
                .collect(Collectors.toMap(Label::getName, Label::getValue));
    }

    private JacksonGauge createGaugeMetric(final String name, Instant t) {
        if (t == null) {
            t = Instant.now().plusSeconds(10);
        }
        return JacksonGauge.builder()
            .withName(name)
            .withDescription("Test Gauge Metric")
            .withTimeReceived(Instant.now())
            .withTime(t.toString())
            .withStartTime(Instant.now().plusSeconds(5).toString())
            .withUnit("1")
            .withValue(1.0d)
            .withEventHandle(eventHandle)
            .build(false);
    }

}
