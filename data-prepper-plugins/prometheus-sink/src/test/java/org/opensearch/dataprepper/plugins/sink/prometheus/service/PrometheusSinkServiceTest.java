/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

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
import org.mockito.Mockito;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.prometheus.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.dlq.DlqPushHandler;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.lenient;

public class PrometheusSinkServiceTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private static final String SINK_YAML =
            "        url: \"http://localhost:8080/test\"\n" +
            "        proxy: \"http://localhost:8080/proxy\"\n" +
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
            "        aws:\n" +
            "          region: \"us-east-2\"\n" +
            "          sts_role_arn: \"arn:aws:iam::895099425785:role/data-prepper-s3source-execution-role\"\n" +
            "          sts_external_id: \"test-external-id\"\n" +
            "          sts_header_overrides: {\"test\": test }\n" +
            "        max_retries: 5\n" +
            "        encoding: snappy\n" +
            "        content_type: \"application/octet-stream\"\n" +
            "        remote_write_version: 0.1.0\n";

    private PrometheusSinkConfiguration prometheusSinkConfiguration;

    private DlqPushHandler dlqPushHandler;

    private PluginSetting pluginSetting;

    private HttpClientBuilder httpClientBuilder;

    private PluginMetrics pluginMetrics;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private CloseableHttpClient closeableHttpClient;

    private CloseableHttpResponse closeableHttpResponse;

    private Counter prometheusSinkRecordsSuccessCounter;

    private Counter prometheusSinkRecordsFailedCounter;

    @BeforeEach
    void setup() throws IOException {
        this.pluginMetrics = mock(PluginMetrics.class);
        this.prometheusSinkConfiguration = objectMapper.readValue(SINK_YAML,PrometheusSinkConfiguration.class);
        this.dlqPushHandler = mock(DlqPushHandler.class);
        this.pluginSetting = mock(PluginSetting.class);
        this.httpClientBuilder = mock(HttpClientBuilder.class);
        this.awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        this.closeableHttpClient = mock(CloseableHttpClient.class);
        this.closeableHttpResponse = mock(CloseableHttpResponse.class);
        this.prometheusSinkRecordsSuccessCounter = mock(Counter.class);
        this.prometheusSinkRecordsFailedCounter = mock(Counter.class);
        lenient().when(httpClientBuilder.setConnectionManager(Mockito.any())).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.addResponseInterceptorLast(any(FailedHttpResponseInterceptor.class))).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.build()).thenReturn(closeableHttpClient);
        lenient().when(closeableHttpClient.execute(any(ClassicHttpRequest.class),any(HttpClientContext.class))).thenReturn(closeableHttpResponse);
        when(pluginMetrics.counter(PrometheusSinkService.PROMETHEUS_SINK_RECORDS_SUCCESS_COUNTER)).thenReturn(prometheusSinkRecordsSuccessCounter);
        when(pluginMetrics.counter(PrometheusSinkService.PROMETHEUS_SINK_RECORDS_FAILED_COUNTER)).thenReturn(prometheusSinkRecordsFailedCounter);

    }

    PrometheusSinkService createObjectUnderTest(final int eventCount, final PrometheusSinkConfiguration httpSinkConfig) throws NoSuchFieldException, IllegalAccessException {
        return new PrometheusSinkService(
                httpSinkConfig,
                dlqPushHandler,
                httpClientBuilder,
                pluginMetrics,
                pluginSetting);
    }

    @Test
    void prometheus_sink_service_test_output_with_single_record_for_jackson_gauge() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(1,prometheusSinkConfiguration);
        Map<String,Object> attributeMap = new HashMap<>();
        Map<String,Object> attributeInnerMap = new HashMap<>();
        attributeInnerMap.put("MyInnerLabel", "MyInnerValue");
        attributeMap.put("MyLabelKey","MyLabelValue");
        attributeMap.put("MyLabelMap",attributeInnerMap);
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder().withEventType("METRIC").build();
        Record<Event> eventRecord =new Record<>(JacksonGauge.builder()
                .withName("prometheus")
                .withTime(Instant.ofEpochSecond(0L, System.currentTimeMillis()).toString())
                .withValue(1.1)
                .withAttributes(attributeMap)
                .withData("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}")
                .withEventMetadata(eventMetadata).build());
        Collection<Record<Event>> records = List.of(eventRecord);
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
    }

    @Test
    void prometheus_sink_service_test_output_with_single_record_for_jackson_sum() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(1,prometheusSinkConfiguration);
        Map<String,Object> attributeMap = new HashMap<>();
        Map<String,Object> attributeInnerMap = new HashMap<>();
        attributeInnerMap.put("MyInnerLabel", "MyInnerValue");
        attributeMap.put("MyLabelKey","MyLabelValue");
        attributeMap.put("MyLabelMap",attributeInnerMap);
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder().withEventType("METRIC").build();
        Record<Event> eventRecord =new Record<>(JacksonSum.builder()
                .withName("prometheus")
                .withTime(Instant.ofEpochSecond(0L, System.currentTimeMillis()).toString())
                .withValue(1.1)
                .withAttributes(attributeMap)
                .withIsMonotonic(true)
                .withData("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}")
                .withEventMetadata(eventMetadata).build());
        Collection<Record<Event>> records = List.of(eventRecord);
        objectUnderTest.output(records);
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
    }

    @Test
    void prometheus_sink_service_test_output_with_single_record_for_jackson_histogram() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(1,prometheusSinkConfiguration);
        Map<String,Object> attributeMap = new HashMap<>();
        Map<String,Object> attributeInnerMap = new HashMap<>();
        attributeInnerMap.put("MyInnerLabel", "MyInnerValue");
        attributeMap.put("MyLabelKey","MyLabelValue");
        attributeMap.put("MyLabelMap",attributeInnerMap);
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder().withEventType("METRIC").build();
        Record<Event> eventRecord =new Record<>(JacksonHistogram.builder()
                .withName("prometheus")
                .withTime(Instant.ofEpochSecond(0L, System.currentTimeMillis()).toString())
                .withSum(1.1)
                .withAttributes(attributeMap)
                .withData("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}")
                .withEventMetadata(eventMetadata).build());
        Collection<Record<Event>> records = List.of(eventRecord);
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
    }

    @Test
    void prometheus_sink_service_test_output_with_single_record_for_jackson_exponential_histogram() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(1,prometheusSinkConfiguration);
        Map<String,Object> attributeMap = new HashMap<>();
        Map<String,Object> attributeInnerMap = new HashMap<>();
        attributeInnerMap.put("MyInnerLabel", "MyInnerValue");
        attributeMap.put("MyLabelKey","MyLabelValue");
        attributeMap.put("MyLabelMap",attributeInnerMap);
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder().withEventType("METRIC").build();
        Record<Event> eventRecord =new Record<>(JacksonExponentialHistogram.builder()
                .withName("prometheus")
                .withTime(Instant.ofEpochSecond(0L, System.currentTimeMillis()).toString())
                .withSum(1.1)
                .withAttributes(attributeMap)
                .withData("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}")
                .withEventMetadata(eventMetadata).build());
        Collection<Record<Event>> records = List.of(eventRecord);
        assertDoesNotThrow(() -> { objectUnderTest.output(records);});
    }

    @Test
    void prometheus_sink_service_test_with_internal_server_error() throws NoSuchFieldException, IllegalAccessException, IOException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(1,prometheusSinkConfiguration);
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder().withEventType("METRIC").build();
        Map<String,Object> attributeMap = new HashMap<>();
        attributeMap.put("MyLableKey","MyLableValue");
        Record<Event> eventRecord =new Record<>(JacksonGauge.builder()
                .withName("prometheus")
                .withTime(Instant.now().toString())
                .withValue(1.1)
                .withAttributes(attributeMap)
                .withData("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}")
                .withEventMetadata(eventMetadata).build());
        lenient().when(closeableHttpClient.execute(any(ClassicHttpRequest.class),any(HttpClientContext.class))).thenThrow(new IOException("internal server error"));
        objectUnderTest.output(List.of(eventRecord));
    }

    @Test
    void prometheus_sink_service_test_with_single_record_with_basic_authentication() throws NoSuchFieldException, IllegalAccessException, JsonProcessingException {

        final String basicAuthYaml =            "          http_basic:\n" +
                "            username: \"username\"\n" +
                "            password: \"vip\"\n" ;
        ReflectivelySetField.setField(PrometheusSinkConfiguration.class,prometheusSinkConfiguration,"authentication", objectMapper.readValue(basicAuthYaml, AuthenticationOptions.class));
        ReflectivelySetField.setField(PrometheusSinkConfiguration.class,prometheusSinkConfiguration,"authType", AuthTypeOptions.HTTP_BASIC);
        Map<String,Object> attributeMap = new HashMap<>();
        Map<String,Object> attributeInnerMap = new HashMap<>();
        attributeInnerMap.put("MyInnerLabel", "MyInnerValue");
        attributeMap.put("MyLabelKey","MyLabelValue");
        attributeMap.put("MyLabelMap",attributeInnerMap);
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder().withEventType("METRIC").build();
        Record<Event> eventRecord =new Record<>(JacksonGauge.builder()
                .withName("prometheus")
                .withTime(Instant.ofEpochSecond(0L, System.currentTimeMillis()).toString())
                .withValue(1.1)
                .withAttributes(attributeMap)
                .withData("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}")
                .withEventMetadata(eventMetadata).build());
        lenient().when(httpClientBuilder.setDefaultCredentialsProvider(any(BasicCredentialsProvider.class))).thenReturn(httpClientBuilder);
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(1,prometheusSinkConfiguration);
        assertDoesNotThrow(() -> { objectUnderTest.output(List.of(eventRecord));});
    }

    @Test
    void prometheus_sink_service_test_output_with_zero_record() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService objectUnderTest = createObjectUnderTest(1,prometheusSinkConfiguration);
        Collection<Record<Event>> records = List.of();
        objectUnderTest.output(records);
    }
}
