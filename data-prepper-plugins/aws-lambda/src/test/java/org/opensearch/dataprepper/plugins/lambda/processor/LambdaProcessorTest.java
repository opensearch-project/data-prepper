package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LambdaProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private LambdaProcessorConfig lambdaProcessorConfig;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Counter numberOfRecordsSuccessCounter;

    @Mock
    private Counter numberOfRecordsFailedCounter;

    @Mock
    private Timer lambdaLatencyMetric;

    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    @Mock
    private BufferFactory bufferFactory;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private BatchOptions batchOptions;

    @Mock
    private ThresholdOptions thresholdOptions;

    @Mock
    private LambdaCommonHandler lambdaCommonHandler;

    private LambdaProcessor lambdaProcessor;

    private static final String FUNCTION_NAME = "test-function";
    private static final String INVOCATION_TYPE = "RequestResponse";
    private static final String RESPONSE_PAYLOAD = "{\"result\":\"success\"}";

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(pluginMetrics.counter(any())).thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.timer(any())).thenReturn(lambdaLatencyMetric);
        when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
        when(lambdaProcessorConfig.getSdkTimeout()).thenReturn(Duration.ofSeconds(5));
    }

    private void setUpBatchEventProcessor() {
        // Set up configuration for batch event
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(FUNCTION_NAME);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(INVOCATION_TYPE);
        when(lambdaProcessorConfig.getPayloadModel()).thenReturn(LambdaCommonConfig.BATCH_EVENT);
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);

        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCount()).thenReturn(5);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("5mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(5));

        lambdaProcessor = new LambdaProcessor(pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);
    }

    private void setUpSingleEventProcessor() {
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(FUNCTION_NAME);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(INVOCATION_TYPE);
        when(lambdaProcessorConfig.getPayloadModel()).thenReturn(LambdaCommonConfig.SINGLE_EVENT);

        lambdaProcessor = new LambdaProcessor(pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);
    }

    private LambdaProcessor createObjectUnderTest(LambdaProcessorConfig lambdaProcessorConfig) {
        return new LambdaProcessor(pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);
    }

    @Test
    public void testDoExecute_withEmptyRecords_batchEvent() throws Exception {
        setUpBatchEventProcessor();
        Collection<Record<Event>> emptyRecords = new ArrayList<>();

        Collection<Record<Event>> result = lambdaProcessor.doExecute(emptyRecords);

        // Should return the same empty collection
        assert(result.isEmpty());
        verify(lambdaCommonHandler, never()).processEvent(any(), any());
    }

    @Test
    public void testDoExecute_withEmptyRecords_singleEvent() throws Exception {
        setUpSingleEventProcessor();
        Collection<Record<Event>> emptyRecords = new ArrayList<>();

        Collection<Record<Event>> result = lambdaProcessor.doExecute(emptyRecords);

        assert(result.isEmpty());
        verify(lambdaCommonHandler, never()).processEvent(any(), any());
    }

    @Test
    public void testDoExecute_single_event_WithConfig() throws JsonProcessingException {
        // Arrange: Create a configuration from a YAML string
        final String config = "function_name: test_function\n" +
                "invocation_type: request-response\n" +
                "payload_model: single-event\n" +
                "aws:\n" +
                "  region: us-east-1\n" +
                "  sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
                "  sts_header_overrides: {\"test\":\"test\"}\n" +
                "max_retries: 3\n";

        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        LambdaProcessorConfig lambdaConfig = yamlMapper.readValue(config, LambdaProcessorConfig.class);

        // Create an actual Event using JacksonEvent
        Event event = JacksonEvent.builder().withEventType("event").withData("{\"status\":true}").build();
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = List.of(record);

        // Mocking LambdaClientFactory to return mocked lambdaAsyncClient using Mockito.mockStatic
        try (MockedStatic<LambdaClientFactory> mockedFactory = Mockito.mockStatic(LambdaClientFactory.class)) {
            mockedFactory.when(() -> LambdaClientFactory.createAsyncLambdaClient(
                            any(), anyInt(), any(), any()))
                    .thenReturn(lambdaAsyncClient);

            // Mocking invoke response
            InvokeResponse invokeResponse = InvokeResponse.builder().statusCode(200).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();
            CompletableFuture<InvokeResponse> invokeResponseFuture = CompletableFuture.completedFuture(invokeResponse);
            lenient().when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(invokeResponseFuture);

            // Act: Create LambdaProcessor and execute it with records
            LambdaProcessor lambdaProcessor = createObjectUnderTest(lambdaConfig);
            Collection<Record<Event>> resultRecords = lambdaProcessor.doExecute(records);

            // Assert: Verify interactions and results
            verify(lambdaAsyncClient, times(1)).invoke(any(InvokeRequest.class));
            assertEquals(1, resultRecords.size());
        }
    }

    @Test
    public void testDoExecute_WithBatchConfig() throws JsonProcessingException {
        // Arrange: Create a configuration from a YAML string for batch processing
        final String config = "function_name: test_function\n" +
                "invocation_type: request-response\n" +
                "payload_model: batch-event\n" +
                "aws:\n" +
                "  region: us-east-1\n" +
                "  sts_role_arn: arn:aws:iam::1234:role/app-test\n" +
                "  sts_header_overrides: {\"test\":\"test\"}\n" +
                "max_retries: 3\n" +
                "batch:\n" +
                "  key_name: testKey\n" +
                "  threshold:\n" +
                "    event_count: 5\n" +
                "    maximum_size: 5mb\n" +
                "    event_collect_timeout: PT5s\n";

        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.registerModule(new JavaTimeModule());
        LambdaProcessorConfig lambdaConfig = yamlMapper.readValue(config, LambdaProcessorConfig.class);


        Map<String, Object> eventData1 = Map.of("key", "value1");
        Map<String, Object> eventData2 = Map.of("key", "value2");
        Event event1 = JacksonEvent.builder().withEventType("event").withData(eventData1).build();
        Event event2 = JacksonEvent.builder().withEventType("event").withData(eventData2).build();
        Record<Event> record1 = new Record<>(event1);
        Record<Event> record2 = new Record<>(event2);
        Collection<Record<Event>> records = List.of(record1, record2);

        try (MockedStatic<LambdaClientFactory> mockedFactory = Mockito.mockStatic(LambdaClientFactory.class)) {
            mockedFactory.when(() -> LambdaClientFactory.createAsyncLambdaClient(
                            any(), anyInt(), any(), any()))
                    .thenReturn(lambdaAsyncClient);

            InvokeResponse invokeResponse = InvokeResponse.builder().statusCode(200).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();
            CompletableFuture<InvokeResponse> invokeResponseFuture = CompletableFuture.completedFuture(invokeResponse);
            lenient().when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(invokeResponseFuture);

            // Act
            LambdaProcessor lambdaProcessor = createObjectUnderTest(lambdaConfig);
            Collection<Record<Event>> resultRecords = lambdaProcessor.doExecute(records);

            // Assert
            verify(lambdaAsyncClient, times(1)).invoke(any(InvokeRequest.class));
            assertEquals(1, resultRecords.size());
        }
    }
}
