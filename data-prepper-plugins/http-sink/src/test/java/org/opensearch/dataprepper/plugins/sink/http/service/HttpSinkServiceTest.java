/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.sink.DefaultSinkMetrics;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.http.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.http.HttpEndpointResponse;
import org.opensearch.dataprepper.plugins.sink.http.HttpSinkSender;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.configuration.ThresholdOptions;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class HttpSinkServiceTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private static final String SINK_YAML =
            "        url: \"http://localhost:8080/test\"\n" +
            "        codec:\n" +
            "          ndjson:\n" +
            "        aws:\n" +
            "          region: \"us-east-2\"\n" +
            "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
            "          sts_external_id: \"test-external-id\"\n" +
            "          sts_header_overrides: {\"test\": test }\n" +
            "        threshold:\n" +
            "          max_events: 1\n" +
            "          max_request_size: 2mb\n" +
            "        max_retries: 5\n";

    private OutputCodec codec;

    private HttpSinkConfiguration httpSinkConfiguration;

    private BufferFactory bufferFactory;


    private PluginSetting pluginSetting;

    //private WebhookService webhookService;

    private HttpClientBuilder httpClientBuilder;

    private PluginMetrics pluginMetrics;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private Counter httpSinkRecordsSuccessCounter;

    private Counter httpSinkRecordsFailedCounter;

    private CloseableHttpClient closeableHttpClient;

    private CloseableHttpResponse closeableHttpResponse;

    private PipelineDescription pipelineDescription;

    @BeforeEach
    void setup() throws Exception {
        this.codec = mock(OutputCodec.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.httpSinkConfiguration = objectMapper.readValue(SINK_YAML,HttpSinkConfiguration.class);
        this.pluginSetting = mock(PluginSetting.class);
        //this.webhookService = mock(WebhookService.class);
        this.httpClientBuilder = mock(HttpClientBuilder.class);
        this.awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        this.httpSinkRecordsSuccessCounter = mock(Counter.class);
        this.httpSinkRecordsFailedCounter = mock(Counter.class);
        this.closeableHttpClient = mock(CloseableHttpClient.class);
        this.closeableHttpResponse = mock(CloseableHttpResponse.class);
        this.bufferFactory = new InMemoryBufferFactory();
        this.pipelineDescription = mock(PipelineDescription.class);

        lenient().when(httpClientBuilder.setConnectionManager(Mockito.any())).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.addResponseInterceptorLast(any(FailedHttpResponseInterceptor.class))).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.build()).thenReturn(closeableHttpClient);
        lenient().when(closeableHttpClient.execute(any(ClassicHttpRequest.class),any(HttpClientContext.class))).thenReturn(closeableHttpResponse);
    }

    HttpSinkService createObjectUnderTest(final int eventCount,final HttpSinkConfiguration httpSinkConfig) throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(ThresholdOptions.class,httpSinkConfig.getThresholdOptions(),"flushTimeout", Duration.ofNanos(1));
        ReflectivelySetField.setField(ThresholdOptions.class,httpSinkConfig.getThresholdOptions(),"maxEvents", eventCount);
        HttpSinkSender httpSender = mock(HttpSinkSender.class);
        when(httpSender.send(any(byte[].class))).thenReturn(new HttpEndpointResponse(httpSinkConfig.getUrl(), 200));
        SinkMetrics sinkMetrics = new DefaultSinkMetrics(pluginMetrics, "Event");
        return new HttpSinkService(
                httpSinkConfig,
                sinkMetrics,
                httpSender,
                pipelineDescription,
                codec,
                null);
    }

    @Test
    void http_sink_service_test_output_with_single_record() throws NoSuchFieldException, IllegalAccessException {
        final HttpSinkService objectUnderTest = createObjectUnderTest(1,httpSinkConfiguration);
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        Collection<Record<Event>> records = List.of(eventRecord);
        objectUnderTest.output(records);
    }

    @Test
    void http_sink_service_test_output_with_multiple_records() throws NoSuchFieldException, IllegalAccessException {
        final int sinkRecords = new Random().nextInt(100);
        final HttpSinkService objectUnderTest = createObjectUnderTest(sinkRecords,httpSinkConfiguration);
        Collection<Record<Event>> records = new ArrayList<>(sinkRecords);
        for(int record = 0; sinkRecords > record ; record++)
            records.add(new Record<>(JacksonEvent.fromMessage("{\"message\":" + UUID.randomUUID() + "}")));
        objectUnderTest.output(records);
    }

    @Test
    void http_sink_service_test_with_internal_server_error() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(ThresholdOptions.class,httpSinkConfiguration.getThresholdOptions(),"flushTimeout", Duration.ofNanos(1));
        ReflectivelySetField.setField(ThresholdOptions.class,httpSinkConfiguration.getThresholdOptions(),"maxEvents", 1);
        HttpSinkSender httpSender = mock(HttpSinkSender.class);
        when(httpSender.send(any(byte[].class))).thenReturn(new HttpEndpointResponse(httpSinkConfiguration.getUrl(), 500, "internal server error"));
        SinkMetrics sinkMetrics = new DefaultSinkMetrics(pluginMetrics, "Event");
        final HttpSinkService objectUnderTest = new HttpSinkService(
                httpSinkConfiguration,
                sinkMetrics,
                httpSender,
                pipelineDescription,
                codec,
                null);
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        objectUnderTest.output(List.of(eventRecord));
    }

    @Test
    void http_sink_service_test_output_with_zero_record() throws NoSuchFieldException, IllegalAccessException {
        final HttpSinkService objectUnderTest = createObjectUnderTest(1,httpSinkConfiguration);
        Collection<Record<Event>> records = List.of();
        objectUnderTest.output(records);
        verifyNoMoreInteractions(httpSinkRecordsSuccessCounter);
        verifyNoMoreInteractions(httpSinkRecordsFailedCounter);
    }

    @Test
    void http_sink_service_test_output_with_single_record_ack_release() throws NoSuchFieldException, IllegalAccessException {
        final HttpSinkService objectUnderTest = createObjectUnderTest(1,httpSinkConfiguration);
        final Event event = mock(Event.class);
        given(event.toJsonString()).willReturn("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}");
        given(event.getEventHandle()).willReturn(mock(EventHandle.class));
        given(event.jsonBuilder()).willReturn(mock(Event.JsonStringBuilder.class));
        objectUnderTest.output(List.of(new Record<>(event)));
    }
}
