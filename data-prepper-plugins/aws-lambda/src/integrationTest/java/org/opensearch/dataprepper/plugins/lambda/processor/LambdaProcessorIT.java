/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import org.mockito.Mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.common.util.CountingRetryCondition;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LambdaProcessorIT {
    @Mock
    InvocationType invocationType;
    private AwsCredentialsProvider awsCredentialsProvider;
    private LambdaProcessor lambdaProcessor;
    private LambdaProcessorConfig lambdaProcessorConfig;
    private String functionName;
    private String lambdaRegion;
    private String role;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private Counter numberOfRecordsSuccessCounter;
    @Mock
    private Counter numberOfRequestsSuccessCounter;
    @Mock
    private Counter numberOfRecordsFailedCounter;
    @Mock
    private Counter numberOfRequestsFailedCounter;
    @Mock
    private Counter batchExceedingThresholdCounter;
    @Mock
    private Timer testTimer;

    private LambdaProcessor createObjectUnderTest(LambdaProcessorConfig processorConfig) {
        return new LambdaProcessor(pluginFactory, pluginSetting, processorConfig, awsCredentialsSupplier, expressionEvaluator);
    }

    @BeforeEach
    public void setup() {
        lambdaRegion = System.getProperty("tests.lambda.processor.region");
        functionName = System.getProperty("tests.lambda.processor.functionName");
        role = System.getProperty("tests.lambda.processor.sts_role_arn");

        pluginMetrics = mock(PluginMetrics.class);
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(pluginSetting.getName()).thenReturn("name");
        testTimer = mock(Timer.class);
        when(pluginMetrics.timer(any())).thenReturn(testTimer);
        lambdaProcessorConfig = mock(LambdaProcessorConfig.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        awsCredentialsProvider = DefaultCredentialsProvider.create();
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        pluginFactory = mock(PluginFactory.class);
        JsonInputCodecConfig jsonInputCodecConfig = mock(JsonInputCodecConfig.class);
        when(jsonInputCodecConfig.getKeyName()).thenReturn(null);
        when(jsonInputCodecConfig.getIncludeKeys()).thenReturn(null);
        when(jsonInputCodecConfig.getIncludeKeysMetadata()).thenReturn(null);
        InputCodec responseCodec = new JsonInputCodec(jsonInputCodecConfig);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class))).thenReturn(responseCodec);
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(lambdaProcessorConfig.getWhenCondition()).thenReturn(null);
        BatchOptions batchOptions = mock(BatchOptions.class);
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(null);
        invocationType = mock(InvocationType.class);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(invocationType);
        when(lambdaProcessorConfig.getResponseCodecConfig()).thenReturn(null);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        when(batchOptions.getKeyName()).thenReturn("osi_key");
        when(thresholdOptions.getEventCount()).thenReturn(ThresholdOptions.DEFAULT_EVENT_COUNT);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse(ThresholdOptions.DEFAULT_BYTE_CAPACITY));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(ThresholdOptions.DEFAULT_EVENT_TIMEOUT);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(lambdaRegion));
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(role);
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(null);
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(null);
        when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    }

    private void populatePrivateFields(LambdaProcessor lambdaProcessor) throws Exception {
        // Use reflection to set the private fields
        setPrivateField(lambdaProcessor, "numberOfRecordsSuccessCounter",
                numberOfRecordsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRecordsFailedCounter",
                numberOfRecordsFailedCounter);
        setPrivateField(lambdaProcessor, "numberOfRequestsSuccessCounter",
                numberOfRequestsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRequestsFailedCounter",
                numberOfRequestsFailedCounter);
        setPrivateField(lambdaProcessor, "batchExceedingThresholdCounter",
                batchExceedingThresholdCounter);
    }

    // Helper method to set private fields via reflection
    private void setPrivateField(Object targetObject, String fieldName, Object value)
            throws Exception {
        Field field = targetObject.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(targetObject, value);
    }

    @ParameterizedTest
    //@ValueSource(ints = {2, 5, 10, 100, 1000})
    @ValueSource(ints = {1000})
    public void testRequestResponseWithMatchingEventsStrictMode(int numRecords) {
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true);
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        List<Record<Event>> records = createRecords(numRecords);
        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);
        assertThat(results.size(), equalTo(numRecords));
        validateStrictModeResults(results);
    }

    @ParameterizedTest
    //@ValueSource(ints = {2, 5, 10, 100, 1000})
    @ValueSource(ints = {1000})
    public void testRequestResponseWithMatchingEventsAggregateMode(int numRecords) {
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false);
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        List<Record<Event>> records = createRecords(numRecords);
        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);
        assertThat(results.size(), equalTo(numRecords));
        validateResultsForAggregateMode(results);
    }

    @ParameterizedTest
    @ValueSource(ints = {1000})
    public void testRequestResponse_WithMatchingEvents_StrictMode_WithMultipleThreads(int numRecords) throws Exception {
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true);
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        int numThreads = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<Record<Event>> records = createRecords(numRecords);
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                try {
                    Collection<Record<Event>> results = lambdaProcessor.doExecute(records);
                    assertThat(results.size(), equalTo(numRecords));
                    validateStrictModeResults(results);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(5, TimeUnit.MINUTES);
        executorService.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"RequestResponse", "Event"})
    public void testDifferentInvocationTypes(String invocationType) throws Exception {
        when(this.invocationType.getAwsLambdaValue()).thenReturn(invocationType);
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true);
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        List<Record<Event>> records = createRecords(10);
        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);
        if (invocationType.equals("RequestResponse")) {
            assertThat(results.size(), equalTo(10));
            validateStrictModeResults(results);
        } else {
            // For "Event" invocation type
            assertThat(results.size(), equalTo(10));
        }
    }

    @Test
    public void testWithFailureTags() throws Exception {
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false);
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(Collections.singletonList("lambda_failure"));
        LambdaProcessor spyLambdaProcessor = spy(createObjectUnderTest(lambdaProcessorConfig));
        doThrow(new RuntimeException("Simulated Lambda failure"))
                .when(spyLambdaProcessor).convertLambdaResponseToEvent(any(Buffer.class), any(InvokeResponse.class));
        List<Record<Event>> records = createRecords(5);
        Collection<Record<Event>> results = spyLambdaProcessor.doExecute(records);
        assertThat(results.size(), equalTo(5));
        for (Record<Event> record : results) {
            assertThat(record.getData().getMetadata().getTags().contains("lambda_failure"), equalTo(true));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"returnNull", "returnEmptyArray", "returnString", "returnEmptyMapinArray", "returnNone"})
    public void testAggregateMode_WithVariousResponses(String input) {
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false); // Aggregate mode
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(Collections.singletonList("lambda_failure"));
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        List<Record<Event>> records = createRecord(input);

        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);

        switch (input) {
            case "returnNull":
            case "returnEmptyArray":
            case "returnString":
            case "returnNone":
                assertTrue(results.isEmpty(), "Events should be dropped for null, empty array, or string response");
                break;
            case "returnEmptyMapinArray":
                assertEquals(1, results.size(), "Should have one event in result for empty map in array");
                assertTrue(results.stream().allMatch(record -> record.getData().toMap().isEmpty()),
                        "Result should be an empty map");
                break;
            default:
                fail("Unexpected input: " + input);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"returnNone", "returnString", "returnObject", "returnEmptyArray", "returnNull", "returnEmptyMapinArray"})
    public void testStrictMode_WithVariousResponses(String input) {
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true); // Strict mode
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(Collections.singletonList("lambda_failure"));
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        List<Record<Event>> records = createRecord(input);

        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);

        switch (input) {
            case "returnNone":
            case "returnString":
            case "returnEmptyArray":
            case "returnNull":
                assertEquals(1, results.size(), "Should return original record with failure tag");
                assertTrue(results.iterator().next().getData().getMetadata().getTags().contains("lambda_failure"),
                        "Result should contain lambda_failure tag");
                break;
            case "returnObject":
                assertEquals(1, results.size(), "Should return one record");
                assertEquals(records.get(0).getData().toMap(), results.iterator().next().getData().toMap(),
                        "Returned record should match input record");
                break;
            case "returnEmptyMapinArray":
                assertEquals(1, results.size(), "Should return one record");
                assertTrue(results.iterator().next().getData().toMap().isEmpty(),
                        "Returned record should be an empty map");
                break;
        }
    }

    private List<Record<Event>> createRecord(String input) {
        List<Record<Event>> records = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put(input, 42);
        EventMetadata metadata = DefaultEventMetadata.builder()
                .withEventType("event")
                .build();
        final Event event = JacksonEvent.builder()
                .withData(map)
                .withEventType("event")
                .withEventMetadata(metadata)
                .build();
        records.add(new Record<>(event));

        return records;
    }


    private void validateResultsForAggregateMode(Collection<Record<Event>> results) {
        List<Record<Event>> resultRecords = new ArrayList<>(results);
        for (int i = 0; i < resultRecords.size(); i++) {
            Event event = resultRecords.get(i).getData();
            Map<String, Object> eventData = event.toMap();
            // Check if the event contains the expected data
            assertThat(eventData.containsKey("id"), equalTo(true));
            int id = (Integer) eventData.get("id");
            assertThat(eventData.get("key" + id), equalTo(id));
            String stringValue = "value" + id;
            assertThat(eventData.get("keys" + id), equalTo(stringValue.toUpperCase()));
            // Check that there's no metadata or it's empty
            EventMetadata metadata = event.getMetadata();
            if (metadata != null) {
                assertThat(metadata.getAttributes().isEmpty(), equalTo(true));
                assertThat(metadata.getTags().isEmpty(), equalTo(true));
            }
        }
    }

    private void validateStrictModeResults(Collection<Record<Event>> results) {
        List<Record<Event>> resultRecords = new ArrayList<>(results);
        for (int i = 0; i < resultRecords.size(); i++) {
            Map<String, Object> eventData = resultRecords.get(i).getData().toMap();
            Map<String, Object> attr = resultRecords.get(i).getData().getMetadata().getAttributes();
            int id = (Integer) eventData.get("id");
            assertThat(eventData.get("key" + id), equalTo(id));
            String stringValue = "value" + id;
            assertThat(eventData.get("keys" + id), equalTo(stringValue.toUpperCase()));
            assertThat(attr.get("attr" + id), equalTo(id));
            assertThat(attr.get("attrs" + id), equalTo("attrvalue" + id));
        }
    }

    private List<Record<Event>> createRecords(int numRecords) {
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", i);
            map.put("key" + i, i);
            map.put("keys" + i, "value" + i);
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("attr" + i, i);
            attrs.put("attrs" + i, "attrvalue" + i);
            EventMetadata metadata = DefaultEventMetadata.builder()
                    .withEventType("event")
                    .withAttributes(attrs)
                    .build();
            final Event event = JacksonEvent.builder()
                    .withData(map)
                    .withEventType("event")
                    .withEventMetadata(metadata)
                    .build();
            records.add(new Record<>(event));
        }
        return records;
    }

    @Test
    void testRetryLogicWithThrottlingUsingMultipleThreads() throws Exception {
        /*
         * This test tries to create multiple parallel Lambda invocations
         * while concurrency=1. The first invocation "occupies" the single concurrency slot
         * The subsequent invocations should then get a 429 TooManyRequestsException,
         * triggering our CountingRetryCondition.
         */

        /* Lambda handler function looks like this:
           def lambda_handler(event, context):
                # Simulate a slow operation so that
                # if concurrency = 1, multiple parallel invocations
                # will result in TooManyRequestsException for the second+ invocation.
                time.sleep(10)

                # Return a simple success response
                return {
                    "statusCode": 200,
                    "body": "Hello from concurrency-limited Lambda!"
                }

         */

        functionName = "lambdaExceptionSimulation";
        // Create a CountingRetryCondition
        CountingRetryCondition countingRetryCondition = new CountingRetryCondition();

        // Configure a LambdaProcessorConfig

        // We'll set invocation type to RequestResponse
        InvocationType invocationType = mock(InvocationType.class);
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(invocationType);

        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        // If your code uses "responseEventsMatch", you can set it:
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true);

        // Set up mock ClientOptions for concurrency + small retries
        ClientOptions clientOptions = mock(ClientOptions.class);
        when(clientOptions.getMaxConnectionRetries()).thenReturn(3); // up to 3 retries
        when(clientOptions.getMaxConcurrency()).thenReturn(5);
        when(clientOptions.getConnectionTimeout()).thenReturn(Duration.ofSeconds(5));
        when(clientOptions.getApiCallTimeout()).thenReturn(Duration.ofSeconds(30));
        when(lambdaProcessorConfig.getClientOptions()).thenReturn(clientOptions);

        // AWS auth
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(lambdaRegion));
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(role);
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(null);
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(null);
        when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);

        // Setup the mock for getProvider
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        // Mock the factory to inject our CountingRetryCondition into the LambdaAsyncClient
        try (MockedStatic<LambdaClientFactory> mockedFactory = mockStatic(LambdaClientFactory.class)) {

            LambdaAsyncClient clientWithCountingCondition = LambdaAsyncClient.builder()
                    .region(Region.of(lambdaRegion))
                    .credentialsProvider(awsCredentialsProvider)
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .retryPolicy(
                                    RetryPolicy.builder()
                                            .retryCondition(countingRetryCondition)
                                            .numRetries(3)
                                            .build()
                            )
                            .build())
                    // netty concurrency = 5 to allow parallel requests
                    .httpClient(NettyNioAsyncHttpClient.builder()
                            .maxConcurrency(5)
                            .build())
                    .build();

            mockedFactory.when(() ->
                            LambdaClientFactory.createAsyncLambdaClient(
                                    any(AwsAuthenticationOptions.class),
                                    any(AwsCredentialsSupplier.class),
                                    any(ClientOptions.class)))
                    .thenReturn(clientWithCountingCondition);

            // 7) Instantiate the real LambdaProcessor
            when(pluginSetting.getName()).thenReturn("lambda-processor");
            when(pluginSetting.getPipelineName()).thenReturn("test-pipeline");
            lambdaProcessor = new LambdaProcessor(
                    pluginFactory,
                    pluginSetting,
                    lambdaProcessorConfig,
                    awsCredentialsSupplier,
                    expressionEvaluator
            );

            // Create multiple parallel tasks to call doExecute(...)
            // Each doExecute() invocation sends records to Lambda in an async manner.
            int parallelInvocations = 5;
            ExecutorService executor = Executors.newFixedThreadPool(parallelInvocations);

            List<Future<Collection<Record<Event>>>> futures = new ArrayList<>();
            for (int i = 0; i < parallelInvocations; i++) {
                // Each subset of records calls the processor
                List<Record<Event>> records = createRecords(2);
                Future<Collection<Record<Event>>> future = executor.submit(() -> {
                    return lambdaProcessor.doExecute(records);
                });
                futures.add(future);
            }

            // Wait for all tasks to complete
            executor.shutdown();
            boolean finishedInTime = executor.awaitTermination(5, TimeUnit.MINUTES);
            if (!finishedInTime) {
                throw new RuntimeException("Test timed out waiting for executor tasks to complete.");
            }

            // Check results or handle exceptions
            for (Future<Collection<Record<Event>>> f : futures) {
                try {
                    Collection<Record<Event>> out = f.get();
                } catch (ExecutionException ee) {
                    // A 429 from AWS will be thrown as TooManyRequestsException
                    // If all retries failed, we might see an exception here.
                }
            }

            // Finally, check that we had at least one retry
            // If concurrency=1 is truly enforced, at least some calls should have gotten a 429
            // -> triggered CountingRetryCondition
            int retryCount = countingRetryCondition.getRetryCount();
            assertTrue(
                    retryCount > 0,
                    "Should have at least one retry due to concurrency-based throttling (429)."
            );
        }
    }

    @Test
    public void testLargePayloadBehaviourException() throws Exception {
        ClientOptions clientOptions = mock(ClientOptions.class);
        when(clientOptions.getMaxConnectionRetries()).thenReturn(3); // up to 3 retries
        when(clientOptions.getMaxConcurrency()).thenReturn(5);
        when(clientOptions.getConnectionTimeout()).thenReturn(Duration.ofSeconds(5));
        when(clientOptions.getApiCallTimeout()).thenReturn(Duration.ofSeconds(30));
        LambdaAsyncClient lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
                lambdaProcessorConfig.getAwsAuthenticationOptions(),
                awsCredentialsSupplier,
                clientOptions
        );

        // Configure the mocked ThresholdOptions to use our threshold.
        final BatchOptions batchOptions = lambdaProcessorConfig.getBatchOptions();
        final ThresholdOptions thresholdOptions = batchOptions.getThresholdOptions();
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("5mb"));


        final int largeEventSizeBytes = 10 * 1024 * 1024;
        final List<Record<Event>> records = new ArrayList<>();
        Record record  = createLargeRecord(largeEventSizeBytes);
        records.add(record);

        List<Map<String, Object>> eventDataList = records.stream()
                .map(rec -> rec.getData().toMap())
                .collect(Collectors.toList());
        SdkBytes sdkBytes = SdkBytes.fromByteArray(new ObjectMapper().writeValueAsBytes(eventDataList));
        InvokeRequest requestPayload = InvokeRequest.builder()
                .invocationType("RequestResponse")
                .functionName(functionName)
                .payload(sdkBytes)
                .build();
        CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(requestPayload);
        //Lambda should throw an exception
        ExecutionException executionException = assertThrows(ExecutionException.class,()-> future.get());
        // Assert that the underlying cause is a LambdaException with a payload size error message.
        Throwable cause = executionException.getCause();
        assertNotNull(cause);
        assertTrue(cause instanceof software.amazon.awssdk.services.lambda.model.LambdaException);
    }

    @Test
    public void testLargePayloadBatching() throws Exception {
        final BatchOptions batchOptions = lambdaProcessorConfig.getBatchOptions();
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true); // Strict mode
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(Collections.singletonList("lambda_failure"));

        final ThresholdOptions thresholdOptions = batchOptions.getThresholdOptions();
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(100));
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("5mb"));
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        populatePrivateFields(lambdaProcessor);

        List<Record<Event>> records = new ArrayList<>();
        int twoMB = 2*1024*1024;
        // 5 mb limit and 3 2MB payloads should create 2 separate batches
        for(int i = 0; i < 3; i++){
            records.add(createLargeRecord(twoMB));
        }

        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);

        assertEquals(3, results.size());
        verify(numberOfRequestsSuccessCounter, times(2)).increment();
        verify(numberOfRequestsFailedCounter, never()).increment();
    }

    @Test
    public void testLargeMultiplePayloadBatching() throws Exception {
        final BatchOptions batchOptions = lambdaProcessorConfig.getBatchOptions();
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true); // Strict mode
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(Collections.singletonList("lambda_failure"));

        final ThresholdOptions thresholdOptions = batchOptions.getThresholdOptions();
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(100));
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("5mb"));
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        populatePrivateFields(lambdaProcessor);

        int oneMB = 1*1024*1024;
        int sevenMB = 7*1024*1024;

        Record<Event> oneMbrecord = createLargeRecord(oneMB);
        Record<Event> sevenMbrecord = createLargeRecord(sevenMB);

        List<Record<Event>> records = List.of(oneMbrecord,sevenMbrecord,oneMbrecord);
        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);

        assertEquals(3, results.size());
        verify(numberOfRequestsSuccessCounter, times(2)).increment();
        verify(numberOfRequestsFailedCounter, times(1)).increment();
        verify(batchExceedingThresholdCounter, times(1)).increment();
    }

    @Test
    public void testPayloadSizeBasedBatching() throws Exception {
        final BatchOptions batchOptions = lambdaProcessorConfig.getBatchOptions();
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true); // Strict mode
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(Collections.singletonList("lambda_failure"));

        final ThresholdOptions thresholdOptions = batchOptions.getThresholdOptions();
        when(thresholdOptions.getEventCount()).thenReturn(1000);
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(100));
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("5mb"));
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        populatePrivateFields(lambdaProcessor);

        List<Record<Event>> records = new ArrayList<>();
        int kb_100 = 100*1024;

        for(int i = 0; i < 100; i++){
            records.add(createLargeRecord(kb_100));
        }

        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);

        assertEquals(100, results.size());
        verify(numberOfRequestsSuccessCounter, times(2)).increment();
        verify(numberOfRequestsFailedCounter, never()).increment();
    }

    @Test
    public void testPayloadEventBasedBatching() throws Exception {
        final BatchOptions batchOptions = lambdaProcessorConfig.getBatchOptions();
        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true); // Strict mode
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(Collections.singletonList("lambda_failure"));

        final ThresholdOptions thresholdOptions = batchOptions.getThresholdOptions();
        when(thresholdOptions.getEventCount()).thenReturn(100);
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(100));
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("5mb"));
        lambdaProcessor = createObjectUnderTest(lambdaProcessorConfig);
        populatePrivateFields(lambdaProcessor);

        List<Record<Event>> records = createRecords(2000);

        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);

        assertEquals(2000, results.size());
        verify(numberOfRequestsSuccessCounter, times(20)).increment();
        verify(numberOfRequestsFailedCounter, never()).increment();
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
