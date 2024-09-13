/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LambdaSinkServiceTest {

    public static final int maxEvents = 10;
    public static final int maxRetries = 3;
    public static final String region = "us-east-1";
    public static final String maxSize = "1kb";
    public static final String functionName = "testFunction";
    public static final String invocationType = "event";
    public static final String keyName ="lambda_batch_key";
    public static final String config =
            "        function_name: testFunction\n" +
                    "        aws:\n" +
                    "          region: us-east-1\n" +
                    "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                    "          sts_header_overrides: {\"test\":\"test\"}\n" +
                    "        payload_model: single-event\n"+
                    "        max_retries: 10\n";

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));
    private LambdaSinkConfig lambdaSinkConfig;
    private LambdaClient lambdaClient;
    private PluginMetrics pluginMetrics;
    private Counter numberOfRecordsSuccessCounter;
    private Counter numberOfRecordsFailedCounter;
    private DlqPushHandler dlqPushHandler;
    private Buffer buffer;
    private BufferFactory bufferFactory;
    private OutputCodecContext outputCodecContext;


    private InvokeResponse invokeResponse;

    private SdkHttpResponse sdkHttpResponse;

    InvokeResponse mockResponse;

    @BeforeEach
    public void setup() throws IOException {
        this.lambdaClient = mock(LambdaClient.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.buffer = mock(InMemoryBuffer.class);
        this.lambdaSinkConfig = mock(LambdaSinkConfig.class);
        this.numberOfRecordsSuccessCounter = mock(Counter.class);
        this.numberOfRecordsFailedCounter = mock(Counter.class);
        this.dlqPushHandler = mock(DlqPushHandler.class);
        this.bufferFactory = mock(BufferFactory.class);
        this.outputCodecContext = mock(OutputCodecContext.class);
        when(lambdaSinkConfig.getPayloadModel()).thenReturn("single-event");
        when(pluginMetrics.counter(LambdaSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS)).thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(LambdaSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED)).thenReturn(numberOfRecordsFailedCounter);
        mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();

    }

    private LambdaSinkService createObjectUnderTest(LambdaSinkConfig lambdaSinkConfig) throws IOException {
        bufferFactory = new InMemoryBufferFactory();
        buffer = bufferFactory.getBuffer(lambdaClient,functionName,invocationType);
        when(outputCodecContext.getIncludeKeys()).thenReturn(Collections.emptyList());
        when(outputCodecContext.getExcludeKeys()).thenReturn(Collections.emptyList());
        when(outputCodecContext.getTagsTargetKey()).thenReturn(null);
        return new LambdaSinkService(lambdaClient,
                lambdaSinkConfig,
                pluginMetrics,
                mock(PluginFactory.class),
                mock(PluginSetting.class),
                outputCodecContext,
                mock(AwsCredentialsSupplier.class),
                dlqPushHandler,
                bufferFactory);
    }

    private LambdaSinkService createObjectUnderTest(String config) throws IOException {
        this.lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        bufferFactory = new InMemoryBufferFactory();
        buffer = bufferFactory.getBuffer(lambdaClient,functionName,invocationType);
        when(outputCodecContext.getIncludeKeys()).thenReturn(Collections.emptyList());
        when(outputCodecContext.getExcludeKeys()).thenReturn(Collections.emptyList());
        when(outputCodecContext.getTagsTargetKey()).thenReturn(null);
        return new LambdaSinkService(lambdaClient,
                lambdaSinkConfig,
                pluginMetrics,
                mock(PluginFactory.class),
                mock(PluginSetting.class),
                outputCodecContext,
                mock(AwsCredentialsSupplier.class),
                dlqPushHandler,
                bufferFactory);
    }

    @Test
    public void lambda_sink_test_with_empty_payload_records() throws IOException {
        numberOfRecordsSuccessCounter = mock(Counter.class);
        LambdaSinkService lambdaSinkService = createObjectUnderTest(config);
        lambdaSinkService.output(List.of());
        verifyNoInteractions(lambdaClient);
        verifyNoInteractions(numberOfRecordsSuccessCounter);
        verifyNoInteractions(numberOfRecordsFailedCounter);
    }


    @Test
    public void lambda_sink_test_with_single_record_success_push_to_lambda() throws IOException {
        LambdaSinkService lambdaSinkService = createObjectUnderTest(config);

        Map<String,Object> map = new HashMap<>();
        map.put("query1","test1");
        map.put("query2","test2");

        final Record<Event> eventRecord = new Record<>(JacksonEvent.builder().withData(map).withEventType("event").build());
        Collection<Record<Event>> records = List.of(eventRecord);
        final ArgumentCaptor<InvokeRequest> invokeRequestCaptor = ArgumentCaptor.forClass(InvokeRequest.class);

        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);
        lambdaSinkService.output(records);

        verify(lambdaClient).invoke(invokeRequestCaptor.capture());
        final InvokeRequest actualRequest = invokeRequestCaptor.getValue();
        assertEquals(actualRequest.functionName(), "testFunction");
        assertEquals(actualRequest.invocationType().toString(), "Event");
        verify(numberOfRecordsSuccessCounter).increment(records.size());
    }

    @Test
    public void lambda_sink_test_max_retires_works() throws IOException {
        final String config =
                "        function_name: test_function\n" +
                        "        aws:\n" +
                        "          region: us-east-1\n" +
                        "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                        "          sts_header_overrides: {\"test\":\"test\"}\n" +
                        "        payload_model: single-event\n"+
                        "        max_retries: 3\n";
        this.buffer = mock(InMemoryBuffer.class);
        when(lambdaClient.invoke(any(InvokeRequest.class))).thenThrow(AwsServiceException.class);
        doNothing().when(dlqPushHandler).perform(any(PluginSetting.class), any(LambdaSinkFailedDlqData.class));

        this.lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        bufferFactory = mock(BufferFactory.class);
        buffer = mock(Buffer.class);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        when(buffer.getOutputStream()).thenReturn(byteArrayOutputStream);
        when(bufferFactory.getBuffer(any(LambdaClient.class),any(),any())).thenReturn(buffer);
        doThrow(AwsServiceException.class).when(buffer).flushToLambdaAsync();

        LambdaSinkService lambdaSinkService = new LambdaSinkService(lambdaClient,
                lambdaSinkConfig,
                pluginMetrics,
                mock(PluginFactory.class),
                mock(PluginSetting.class),
                outputCodecContext,
                mock(AwsCredentialsSupplier.class),
                dlqPushHandler,
                bufferFactory);

        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        Collection<Record<Event>> records = List.of(eventRecord);
        lambdaSinkService.output(records);

        verify(buffer, times(3)).flushToLambdaAsync();
    }

    @Test
    public void lambda_sink_test_dlq_works() throws IOException {
        final String config =
                "        function_name: test_function\n" +
                        "        aws:\n" +
                        "          region: us-east-1\n" +
                        "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                        "          sts_header_overrides: {\"test\":\"test\"}\n" +
                        "        payload_model: single-event\n"+
                        "        max_retries: 3\n";

        when(lambdaClient.invoke(any(InvokeRequest.class))).thenThrow(AwsServiceException.class);
        doNothing().when(dlqPushHandler).perform(any(PluginSetting.class), any(LambdaSinkFailedDlqData.class));

        this.lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        bufferFactory = mock(BufferFactory.class);
        buffer = mock(Buffer.class);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        when(buffer.getOutputStream()).thenReturn(byteArrayOutputStream);
        when(bufferFactory.getBuffer(any(LambdaClient.class),any(),any())).thenReturn(buffer);

        doThrow(AwsServiceException.class).when(buffer).flushToLambdaAsync();

        LambdaSinkService lambdaSinkService = new LambdaSinkService(lambdaClient,
                lambdaSinkConfig,
                pluginMetrics,
                mock(PluginFactory.class),
                mock(PluginSetting.class),
                outputCodecContext,
                mock(AwsCredentialsSupplier.class),
                dlqPushHandler,
                bufferFactory);

        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        Collection<Record<Event>> records = List.of(eventRecord);

        lambdaSinkService.output(records);

        verify(buffer, times(3)).flushToLambdaAsync();
        verify(dlqPushHandler,times(1)).perform(any(PluginSetting.class),any(Object.class));
    }

    @Test
    public void lambda_sink_test_with_multiple_record_success_push_to_lambda() throws IOException {
        LambdaSinkService lambdaSinkService = createObjectUnderTest(config);
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        Collection<Record<Event>> records = new ArrayList<>();
        int totalRecords = 11;
        for(int recordSize = 0; recordSize < totalRecords ; recordSize++) {
            records.add(eventRecord);
        }
        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);

        lambdaSinkService.output(records);

        verify(lambdaClient,times(totalRecords)).invoke(any(InvokeRequest.class));

    }

    @Test
    void lambda_sink_service_test_output_with_single_record_ack_release() throws IOException {
        final LambdaSinkService lambdaSinkService = createObjectUnderTest(config);
        final Event event = mock(Event.class);
        given(event.toJsonString()).willReturn("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}");
        given(event.getEventHandle()).willReturn(mock(EventHandle.class));
        final Event.JsonStringBuilder jsonStringBuilder = mock(Event.JsonStringBuilder.class); // Mock the JsonStringBuilder
        given(event.jsonBuilder()).willReturn(jsonStringBuilder);
        given(jsonStringBuilder.includeKeys(outputCodecContext.getIncludeKeys())).willReturn(jsonStringBuilder);
        given(jsonStringBuilder.excludeKeys(outputCodecContext.getExcludeKeys())).willReturn(jsonStringBuilder);
        given(jsonStringBuilder.includeTags(outputCodecContext.getTagsTargetKey())).willReturn(jsonStringBuilder);
        given(jsonStringBuilder.toJsonString()).willReturn("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}");


        final ArgumentCaptor<InvokeRequest> invokeRequestCaptor = ArgumentCaptor.forClass(InvokeRequest.class);
        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);

        lambdaSinkService.output(List.of(new Record<>(event)));

        verify(lambdaClient,times(1)).invoke(invokeRequestCaptor.capture());
        final InvokeRequest actualRequest = invokeRequestCaptor.getValue();
        assertThat(actualRequest.functionName(), equalTo("testFunction"));
        assertThat(actualRequest.invocationType().toString(), equalTo("Event"));
        verify(numberOfRecordsSuccessCounter).increment(1);
    }

    @Test
    public void lambda_sink_test_batch_enabled() throws IOException {
        when(lambdaSinkConfig.getFunctionName()).thenReturn(functionName);
        when(lambdaSinkConfig.getMaxConnectionRetries()).thenReturn(maxRetries);
        when(lambdaSinkConfig.getPayloadModel()).thenReturn("batch-event");
        when(lambdaSinkConfig.getBatchOptions()).thenReturn(mock(BatchOptions.class));
        when(lambdaSinkConfig.getBatchOptions().getKeyName()).thenReturn("lambda_batch_key");
        when(lambdaSinkConfig.getBatchOptions().getThresholdOptions()).thenReturn(mock(ThresholdOptions.class));
        when(lambdaSinkConfig.getBatchOptions().getThresholdOptions().getEventCount()).thenReturn(1);
        when(lambdaSinkConfig.getBatchOptions().getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse(maxSize));
        when(lambdaSinkConfig.getBatchOptions().getThresholdOptions().getEventCollectTimeOut()).thenReturn(Duration.ofNanos(10L));
        when(lambdaSinkConfig.getAwsAuthenticationOptions()).thenReturn(mock(AwsAuthenticationOptions.class));

        LambdaSinkService lambdaSinkService = createObjectUnderTest(lambdaSinkConfig);

        Map<String,Object> map = new HashMap<>();
        map.put("query1","test1");
        map.put("query2","test2");

        String expected_payload = "{\"lambda_batch_key\":[{\"query1\":\"test1\",\"query2\":\"test2\"}]}";
        final Record<Event> eventRecord = new Record<>(JacksonEvent.builder().withData(map).withEventType("event").build());
        Collection<Record<Event>> records = List.of(eventRecord);
        final ArgumentCaptor<InvokeRequest> invokeRequestCaptor = ArgumentCaptor.forClass(InvokeRequest.class);

        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);
        lambdaSinkService.output(records);

        verify(lambdaClient).invoke(invokeRequestCaptor.capture());
        final InvokeRequest actualRequest = invokeRequestCaptor.getValue();
        assertEquals(actualRequest.functionName(), functionName);
        assertEquals(actualRequest.invocationType().toString(), "Event");
        String actualRequestPayload = actualRequest.payload().asUtf8String();
        assertEquals(actualRequestPayload, expected_payload );
        verify(numberOfRecordsSuccessCounter).increment(records.size());
    }
}
