package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.LAMBDA_LATENCY_METRIC;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.REQUEST_PAYLOAD_SIZE;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.RESPONSE_PAYLOAD_SIZE;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@ExtendWith(MockitoExtension.class)
public class LambdaProcessorTest {
    private static final String RESPONSE_PAYLOAD = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
    private static MockedStatic<LambdaClientFactory> lambdaClientFactoryMockedStatic;
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

//    @Mock
//    private PluginSetting pluginSetting;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private LambdaProcessorConfig lambdaProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private LambdaClient lambdaClient;

    @Spy
    private BufferFactory bufferFactory;

    @Mock
    private Buffer buffer;

    @Mock
    private Counter numberOfRecordsSuccessCounter;

    @Mock
    private Counter numberOfRecordsFailedCounter;

    @Mock
    private Counter numberOfRecordsDroppedCounter;

    @Mock
    private Timer lambdaLatencyMetric;

    @Mock
    private AtomicLong requestPayload;

    @Mock
    private AtomicLong responsePayload;

    private LambdaProcessor createObjectUnderTest() {
        return new LambdaProcessor(pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);
    }

    @BeforeEach
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        BatchOptions batchOptions = mock(BatchOptions.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);

        lenient().when(lambdaProcessorConfig.getFunctionName()).thenReturn("test-function1");
        lenient().when(lambdaProcessorConfig.getMaxConnectionRetries()).thenReturn(3);
        lenient().when(lambdaProcessorConfig.getInvocationType()).thenReturn("requestresponse");

        lenient().when(thresholdOptions.getEventCount()).thenReturn(10);
        lenient().when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.ofBytes(6));
        lenient().when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(5));

        lenient().when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        lenient().when(batchOptions.getBatchKey()).thenReturn("key");
        lenient().when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);

        lenient().when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        lenient().when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("test-region"));

        lenient().when(pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS)).thenReturn(numberOfRecordsDroppedCounter);
        lenient().when(pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED)).thenReturn(numberOfRecordsFailedCounter);
        lenient().when(pluginMetrics.timer(LAMBDA_LATENCY_METRIC)).thenReturn(lambdaLatencyMetric);
        lenient().when(pluginMetrics.gauge(eq(REQUEST_PAYLOAD_SIZE), any(AtomicLong.class))).thenReturn(requestPayload);
        lenient().when(pluginMetrics.gauge(eq(RESPONSE_PAYLOAD_SIZE), any(AtomicLong.class))).thenReturn(responsePayload);

        InvokeResponse resp = InvokeResponse.builder().statusCode(200).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();
        lambdaClientFactoryMockedStatic = Mockito.mockStatic(LambdaClientFactory.class);
        when(LambdaClientFactory.createLambdaClient(any(AwsAuthenticationOptions.class),
                eq(3),
                any(AwsCredentialsSupplier.class))).thenReturn(lambdaClient);
        lenient().when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(resp);
        try {
            lenient().when(bufferFactory.getBuffer(lambdaClient, lambdaProcessorConfig.getFunctionName(), "RequestResponse")).thenReturn(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void cleanup() {
        lambdaClientFactoryMockedStatic.close();
    }

    @Test
    public void testDoExecuteWithEmptyRecords() {
        Collection<Record<Event>> records = Collections.emptyList();
        LambdaProcessor lambdaProcessor = createObjectUnderTest();
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testDoExecute() throws JsonProcessingException {
        Event event = JacksonEvent.builder().withEventType("event").withData("{\"status\":true}").build();
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = List.of(record);

        InvokeResponse invokeResponse = InvokeResponse.builder().statusCode(200).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();

        LambdaProcessor lambdaProcessor = createObjectUnderTest();
        Collection<Record<Event>> resultRecords = lambdaProcessor.doExecute(records);

        assertEquals(1, resultRecords.size());
        Record<Event> resultRecord = resultRecords.iterator().next();

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode responseJsonNode = objectMapper.readTree(RESPONSE_PAYLOAD);
        assertEquals(responseJsonNode, resultRecord.getData().getJsonNode());
    }

    @Test
    public void testDoExecute_withException() {
        List<Record<Event>> records = new ArrayList<>();
        Event event = mock(Event.class);
        records.add(new Record<>(event));

        lenient().when(buffer.getOutputStream()).thenThrow(new RuntimeException("Test exception"));

        LambdaProcessor lambdaProcessor = createObjectUnderTest();
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        assertEquals(1, result.size());
        verify(buffer, times(0)).flushToLambdaSync();
    }

    @Test
    public void testFlushToLambdaIfNeeded_withThresholdNotExceeded() throws Exception {
        lenient().when(buffer.getSize()).thenReturn(100L);
        lenient().when(buffer.getEventCount()).thenReturn(1);
        lenient().when(buffer.getDuration()).thenReturn(Duration.ofSeconds(1));

        LambdaProcessor lambdaProcessor = createObjectUnderTest();
        List<Record<Event>> records = mock(ArrayList.class);
        lambdaProcessor.flushToLambdaIfNeeded(records);
        verify(buffer, times(0)).flushToLambdaSync();
        verify(records, times(0)).add(any(Record.class));
    }

    @Test
    public void testConvertLambdaResponseToEvent_withNon200StatusCode() {
        InvokeResponse response = InvokeResponse.builder().statusCode(500).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();
        lenient().when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(response);

        LambdaProcessor lambdaProcessor = createObjectUnderTest();

        Exception exception = assertThrows(RuntimeException.class, () -> {
            lambdaProcessor.convertLambdaResponseToEvent(response);
        });
        assertEquals("Error converting Lambda response to Event", exception.getMessage());
    }

    @Test
    public void testDoExecute_withNonSuccessfulStatusCode() {
        InvokeResponse response = InvokeResponse.builder().statusCode(500).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();
        lenient().when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(response);

        LambdaProcessor lambdaProcessor = createObjectUnderTest();

        List<Record<Event>> records = new ArrayList<>();
        Event event = mock(Event.class);
        records.add(new Record<>(event));
        List<Record<Event>> resultRecords = (List<Record<Event>>) lambdaProcessor.doExecute(records);

        verify(lambdaClient, times(1)).invoke(any(InvokeRequest.class));

        //event should be dropped on failure
        assertEquals(resultRecords.size(), 0);
        verify(numberOfRecordsFailedCounter, times(1)).increment(1);
        //check if buffer is reset
        assertEquals(buffer.getSize(), 0);
    }

    @Test
    public void testConvertLambdaResponseToEvent() throws JsonProcessingException {
        InvokeResponse response = InvokeResponse.builder().statusCode(200).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();
        lenient().when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(response);

        LambdaProcessor lambdaProcessor = createObjectUnderTest();
        Event eventResponse = lambdaProcessor.convertLambdaResponseToEvent(response);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(RESPONSE_PAYLOAD);
        Event event = JacksonEvent.builder().withEventType("event").withData(jsonNode).build();
        assertEquals(event.getJsonNode(), eventResponse.getJsonNode());
    }

    @Test
    public void testDoExecute_WithConfig() throws JsonProcessingException {
        final String config = "        function_name: test_function\n" + "        invocation_type: requestresponse\n" + "        aws:\n" + "          region: us-east-1\n" + "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" + "          sts_header_overrides: {\"test\":\"test\"}\n" + "        max_retries: 3\n";

        this.lambdaProcessorConfig = objectMapper.readValue(config, LambdaProcessorConfig.class);

        Event event = JacksonEvent.builder().withEventType("event").withData("{\"status\":true}").build();
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = List.of(record);

        InvokeResponse invokeResponse = InvokeResponse.builder().statusCode(200).payload(SdkBytes.fromUtf8String(RESPONSE_PAYLOAD)).build();

        LambdaProcessor lambdaProcessor = createObjectUnderTest();
        Collection<Record<Event>> resultRecords = lambdaProcessor.doExecute(records);
        verify(lambdaClient, times(1)).invoke(any(InvokeRequest.class));
        assertEquals(resultRecords.size(), 1);
    }
}
