/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.http.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.http.configuration.AuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.http.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.configuration.ThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.http.dlq.DlqPushHandler;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.io.IOException;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class HttpSinkServiceTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private static final String SINK_YAML =
            "        url: \"http://localhost:8080/test\"\n" +
            "        proxy: \"http://localhost:8080/proxy\"\n" +
            "        codec:\n" +
            "          ndjson:\n" +
            "        http_method: \"POST\"\n" +
            "        auth_type: \"unauthenticated\"\n" +
            "        authentication:\n" +
            "          http_basic:\n" +
            "            username: \"username\"\n" +
            "            password: \"vip\"\n" +
            "          bearer_token:\n" +
            "            client_id: 0oaafr4j79segrYGC5d7\n" +
            "            client_secret: fFel-3FutCXAOndezEsOVlght6D6DR4OIt7G5D1_oJ6w0wNoaYtgU17JdyXmGf0M\n" +
            "            token_url: https://localhost/oauth2/default/v1/token\n" +
            "            grant_type: client_credentials\n" +
            "            scope: httpSink\n"+
            "        insecure_skip_verify: true\n" +
            "        dlq_file: \"/your/local/dlq-file\"\n" +
            "        dlq:\n" +
            "        ssl_certificate_file: \"/full/path/to/certfile.crt\"\n" +
            "        ssl_key_file: \"/full/path/to/keyfile.key\"\n" +
            "        buffer_type: \"in_memory\"\n" +
            "        aws:\n" +
            "          region: \"us-east-2\"\n" +
            "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
            "          sts_external_id: \"test-external-id\"\n" +
            "          sts_header_overrides: {\"test\": test }\n" +
            "        threshold:\n" +
            "          event_count: 1\n" +
            "          maximum_size: 2mb\n" +
            "        max_retries: 5\n" +
            "        aws_sigv4: false\n";

    private OutputCodec codec;

    private HttpSinkConfiguration httpSinkConfiguration;

    private BufferFactory bufferFactory;

    private DlqPushHandler dlqPushHandler;

    private PluginSetting pluginSetting;

    private WebhookService webhookService;

    private HttpClientBuilder httpClientBuilder;

    private PluginMetrics pluginMetrics;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private Counter httpSinkRecordsSuccessCounter;

    private Counter httpSinkRecordsFailedCounter;

    private CloseableHttpClient closeableHttpClient;

    private CloseableHttpResponse closeableHttpResponse;

    @BeforeEach
    void setup() throws IOException {
        this.codec = mock(OutputCodec.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.httpSinkConfiguration = objectMapper.readValue(SINK_YAML,HttpSinkConfiguration.class);
        this.dlqPushHandler = mock(DlqPushHandler.class);
        this.pluginSetting = mock(PluginSetting.class);
        this.webhookService = mock(WebhookService.class);
        this.httpClientBuilder = mock(HttpClientBuilder.class);
        this.awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        this.httpSinkRecordsSuccessCounter = mock(Counter.class);
        this.httpSinkRecordsFailedCounter = mock(Counter.class);
        this.closeableHttpClient = mock(CloseableHttpClient.class);
        this.closeableHttpResponse = mock(CloseableHttpResponse.class);
        this.bufferFactory = new InMemoryBufferFactory();

        lenient().when(httpClientBuilder.setConnectionManager(null)).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.addResponseInterceptorLast(any(FailedHttpResponseInterceptor.class))).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.build()).thenReturn(closeableHttpClient);
        lenient().when(closeableHttpClient.execute(any(ClassicHttpRequest.class),any(HttpClientContext.class))).thenReturn(closeableHttpResponse);
        when(pluginMetrics.counter(HttpSinkService.HTTP_SINK_RECORDS_SUCCESS_COUNTER)).thenReturn(httpSinkRecordsSuccessCounter);
        when(pluginMetrics.counter(HttpSinkService.HTTP_SINK_RECORDS_FAILED_COUNTER)).thenReturn(httpSinkRecordsFailedCounter);

    }

    HttpSinkService createObjectUnderTest(final int eventCount,final HttpSinkConfiguration httpSinkConfig) throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(ThresholdOptions.class,httpSinkConfig.getThresholdOptions(),"eventCollectTimeOut", Duration.ofNanos(1));
        ReflectivelySetField.setField(ThresholdOptions.class,httpSinkConfig.getThresholdOptions(),"eventCount", eventCount);
        return new HttpSinkService(
                httpSinkConfig,
                bufferFactory,
                dlqPushHandler,
                pluginSetting,
                webhookService,
                httpClientBuilder,
                pluginMetrics,
                pluginSetting,
                codec,
                null);
    }

    @Test
    void http_sink_service_test_output_with_single_record() throws NoSuchFieldException, IllegalAccessException {
        final HttpSinkService objectUnderTest = createObjectUnderTest(1,httpSinkConfiguration);
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        Collection<Record<Event>> records = List.of(eventRecord);
        objectUnderTest.output(records);
        verify(httpSinkRecordsSuccessCounter).increment(1);
    }

    @Test
    void http_sink_service_test_output_with_multiple_records() throws NoSuchFieldException, IllegalAccessException {
        final int sinkRecords = new Random().nextInt(100);
        final HttpSinkService objectUnderTest = createObjectUnderTest(sinkRecords,httpSinkConfiguration);
        Collection<Record<Event>> records = new ArrayList<>(sinkRecords);
        for(int record = 0; sinkRecords > record ; record++)
            records.add(new Record<>(JacksonEvent.fromMessage("{\"message\":" + UUID.randomUUID() + "}")));
        objectUnderTest.output(records);
        verify(httpSinkRecordsSuccessCounter).increment(sinkRecords);
    }

    @Test
    void http_sink_service_test_with_internal_server_error() throws NoSuchFieldException, IllegalAccessException, IOException {
        final HttpSinkService objectUnderTest = createObjectUnderTest(1,httpSinkConfiguration);
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        lenient().when(closeableHttpClient.execute(any(ClassicHttpRequest.class),any(HttpClientContext.class))).thenThrow(new IOException("internal server error"));
        objectUnderTest.output(List.of(eventRecord));
        verify(httpSinkRecordsFailedCounter).increment(1);
    }

    @Test
    void http_sink_service_test_with_single_record_with_basic_authentication() throws NoSuchFieldException, IllegalAccessException, JsonProcessingException {

        final String basicAuthYaml =            "          http_basic:\n" +
                "            username: \"username\"\n" +
                "            password: \"vip\"\n" ;
        ReflectivelySetField.setField(HttpSinkConfiguration.class,httpSinkConfiguration,"authentication", objectMapper.readValue(basicAuthYaml, AuthenticationOptions.class));
        ReflectivelySetField.setField(HttpSinkConfiguration.class,httpSinkConfiguration,"authType", AuthTypeOptions.HTTP_BASIC);
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        lenient().when(httpClientBuilder.setDefaultCredentialsProvider(any(BasicCredentialsProvider.class))).thenReturn(httpClientBuilder);
        final HttpSinkService objectUnderTest = createObjectUnderTest(1,httpSinkConfiguration);
        objectUnderTest.output(List.of(eventRecord));
        verify(httpSinkRecordsSuccessCounter).increment(1);
    }

    @Test
    void http_sink_service_test_with_single_record_with_bearer_token() throws NoSuchFieldException, IllegalAccessException, JsonProcessingException {
        lenient().when(httpClientBuilder.setDefaultCredentialsProvider(any(BasicCredentialsProvider.class))).thenReturn(httpClientBuilder);
        final String authentication = "          bearer_token:\n" +
                "            client_id: 0oaafr4j79segrYGC5d7\n" +
                "            client_secret: fFel-3FutCXAOndezEsOVlght6D6DR4OIt7G5D1_oJ6w0wNoaYtgU17JdyXmGf0M\n" +
                "            token_url: https://localhost/oauth2/default/v1/token\n" +
                "            grant_type: client_credentials\n" +
                "            scope: httpSink" ;
        ReflectivelySetField.setField(HttpSinkConfiguration.class,httpSinkConfiguration,"authentication", objectMapper.readValue(authentication, AuthenticationOptions.class));
        final HttpSinkService objectUnderTest = createObjectUnderTest(1,httpSinkConfiguration);
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        objectUnderTest.output(List.of(eventRecord));
        verify(httpSinkRecordsSuccessCounter).increment(1);
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
        verify(httpSinkRecordsSuccessCounter).increment(1);
    }
}
