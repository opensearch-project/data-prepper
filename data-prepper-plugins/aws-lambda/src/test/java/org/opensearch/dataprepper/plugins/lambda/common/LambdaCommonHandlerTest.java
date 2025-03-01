package org.opensearch.dataprepper.plugins.lambda.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessorConfig;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.lambda.utils.LambdaTestSetupUtil.createLambdaConfigurationFromYaml;
import static org.opensearch.dataprepper.plugins.lambda.utils.LambdaTestSetupUtil.getSampleEventRecords;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LambdaCommonHandlerTest {

    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    @Mock
    private OutputCodecContext outputCodecContext;

    @Test
    void testCheckStatusCode() {
        InvokeResponse successResponse = InvokeResponse.builder().statusCode(200).build();
        InvokeResponse failureResponse = InvokeResponse.builder().statusCode(400).build();

        assertTrue(LambdaCommonHandler.isSuccess(successResponse));
        assertFalse(LambdaCommonHandler.isSuccess(failureResponse));
    }

    @Test
    void testWaitForFutures() {
        List<CompletableFuture<InvokeResponse>> futureList = new ArrayList<>();
        CompletableFuture<InvokeResponse> future1 = new CompletableFuture<>();
        CompletableFuture<InvokeResponse> future2 = new CompletableFuture<>();
        futureList.add(future1);
        futureList.add(future2);

        // Simulate completion of futures
        future1.complete(InvokeResponse.builder().build());
        future2.complete(InvokeResponse.builder().build());

        LambdaCommonHandler.waitForFutures(futureList);

        assertFalse(futureList.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"lambda-processor-success-config.yaml"})
    void testSendRecords(String configFilePath) {
        LambdaProcessorConfig lambdaConfiguration = createLambdaConfigurationFromYaml(configFilePath);
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(InvokeResponse.builder().statusCode(200).build()));

        int oneRandomCount = (int) (Math.random() * 1000);
        List<Record<Event>> records = getSampleEventRecords(oneRandomCount);

        Map<Buffer, CompletableFuture<InvokeResponse>> bufferCompletableFutureMap = LambdaCommonHandler.sendRecords(
                records, lambdaConfiguration, lambdaAsyncClient,
                outputCodecContext);

        assertNotNull(bufferCompletableFutureMap);
        int batchSize = lambdaConfiguration.getBatchOptions().getThresholdOptions().getEventCount();
        int bufferBatchCount = (int) Math.ceil((1.0 * oneRandomCount) / batchSize);
        assertEquals(bufferBatchCount,
                bufferCompletableFutureMap.size());
        verify(lambdaAsyncClient, atLeastOnce()).invoke(any(InvokeRequest.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"lambda-processor-null-key-name.yaml"})
    void testSendRecordsWithNullKeyName(String configFilePath) {
        LambdaProcessorConfig lambdaConfiguration = createLambdaConfigurationFromYaml(configFilePath);

        Event mockEvent = mock(Event.class);
        when(mockEvent.toMap()).thenReturn(Collections.singletonMap("testKey", "testValue"));
        List<Record<Event>> records = Collections.singletonList(new Record<>(mockEvent));

        assertThrows(NullPointerException.class, () ->
                LambdaCommonHandler.sendRecords(records, lambdaConfiguration, lambdaAsyncClient, outputCodecContext)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"lambda-processor-success-config.yaml"})
    void testSendRecordsWithFailure(String configFilePath) {
        LambdaProcessorConfig lambdaConfiguration = createLambdaConfigurationFromYaml(configFilePath);
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Test exception")));

        List<Record<Event>> records = new ArrayList<>();
        Map<String, Object> data = Map.of("id", 1);
        Event event = JacksonEvent.builder()
                .withEventType("Event")
                .withData(data)
                .build();
        Record record = new Record<>(event);

        records.add(record);

        LambdaCommonHandler.sendRecords(
                records, lambdaConfiguration, lambdaAsyncClient,
                outputCodecContext);
        verify(lambdaAsyncClient, atLeastOnce()).invoke(any(InvokeRequest.class));
    }

    // Test Payload behavior
    @ParameterizedTest
    @ValueSource(strings = {"lambda-processor-payload-limit.yaml"})
    void testSendRecordsWithPayloadLimit(String configFilePath) {
        // We create two records whose JSON payloads are 60 bytes each.
        // Since 60 + 60 = 120 > 100, they must be batched into separate buffers.

        LambdaProcessorConfig lambdaConfiguration = createLambdaConfigurationFromYaml(configFilePath);
        // The configuration in this YAML should define a maximum payload size of 100 bytes.
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(InvokeResponse.builder().statusCode(200).build()));

        // Create two records with a JSON payload of 60 bytes each.
        String payload = generatePayloadOfSize(60);
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            records.add(createLargeRecord(60));
        }

        Map<Buffer, CompletableFuture<InvokeResponse>> result = LambdaCommonHandler.sendRecords(
                records, lambdaConfiguration, lambdaAsyncClient, outputCodecContext);

        // Expect two separate batches since adding the second record would exceed the payload limit.
        assertEquals(2, result.size(), "Expected 2 batches due to payload limit being exceeded when adding second record");
    }

    // Helper method to generate a string of specified length.
    private String generatePayloadOfSize(int size) {
        return "a".repeat(size);
    }

    private Record<Event> createLargeRecord(final int sizeInBytes) {
        final StringBuilder sb = new StringBuilder(sizeInBytes);
        for (int i = 0; i < sizeInBytes; i++) {
            sb.append("a");
        }
        final String payload = sb.toString();
        final Map<String, Object> data = new HashMap<>();
        data.put("payload", payload);
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("test")
                .build();
        return new Record<>(event);
    }
}
