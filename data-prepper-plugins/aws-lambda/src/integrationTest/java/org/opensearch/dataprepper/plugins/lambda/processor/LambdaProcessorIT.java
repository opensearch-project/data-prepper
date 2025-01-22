/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
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
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.common.util.CustomLambdaRetryCondition;
import org.opensearch.dataprepper.plugins.lambda.utils.CountingHttpClient;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private Counter testCounter;
    @Mock
    private Timer testTimer;

    private LambdaProcessor createObjectUnderTest(LambdaProcessorConfig processorConfig) {
        return new LambdaProcessor(pluginFactory, pluginSetting, processorConfig, awsCredentialsSupplier, expressionEvaluator);
    }

    @BeforeEach
    public void setup() {
//        lambdaRegion = System.getProperty("tests.lambda.processor.region");
//        functionName = System.getProperty("tests.lambda.processor.functionName");
//        role = System.getProperty("tests.lambda.processor.sts_role_arn");
        lambdaRegion = "us-west-2";
        functionName = "lambdaNoReturn";
        role = "arn:aws:iam::176893235612:role/osis-s3-opensearch-role";

        pluginMetrics = mock(PluginMetrics.class);
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(pluginSetting.getName()).thenReturn("name");
        testCounter = mock(Counter.class);
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(testCounter).increment(any(Double.class));
        } catch (Exception e) {
        }
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(testTimer).record(any(Long.class), any(TimeUnit.class));
        } catch (Exception e) {
        }
        when(pluginMetrics.counter(any())).thenReturn(testCounter);
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

    /*
    * For this test, set concurrency limit to 1
     */
    @Test
    void testTooManyRequestsExceptionWithCustomRetryCondition() {
        //Note lambda function for this test looks like this:
        /*def lambda_handler(event, context):
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

        // Wrap the default HTTP client to count requests
        CountingHttpClient countingHttpClient = new CountingHttpClient(
                NettyNioAsyncHttpClient.builder().build()
        );

        // Configure a custom retry policy with 3 retries and your custom condition
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(3)
                .retryCondition(new CustomLambdaRetryCondition())
                .build();

        // Build the real Lambda client
        LambdaAsyncClient client = LambdaAsyncClient.builder()
                .overrideConfiguration(
                        ClientOverrideConfiguration.builder()
                                .retryPolicy(retryPolicy)
                                .build()
                )
                .region(Region.of(lambdaRegion))
                .httpClient(countingHttpClient)
                .build();

        // Parallel invocations to force concurrency=1 to throw TooManyRequestsException
        int parallelInvocations = 10;
        CompletableFuture<?>[] futures = new CompletableFuture[parallelInvocations];
        for (int i = 0; i < parallelInvocations; i++) {
            InvokeRequest request = InvokeRequest.builder()
                    .functionName(functionName)
                    .build();

            futures[i] = client.invoke(request);
        }

        // 5) Wait for all to complete
        CompletableFuture.allOf(futures).join();

        // 6) Check how many had TooManyRequestsException
        long tooManyRequestsCount = Arrays.stream(futures)
                .filter(f -> {
                    try {
                        f.join();
                        return false; // no error => no TMR
                    } catch (CompletionException e) {
                        return e.getCause() instanceof TooManyRequestsException;
                    }
                })
                .count();

        // 7) Observe how many total network requests occurred (including SDK retries)
        int totalRequests = countingHttpClient.getRequestCount();
        System.out.println("Total network requests (including retries): " + totalRequests);

        // Optionally: If you want to confirm the EXACT number,
        // this might vary depending on how many parallel calls and how your TMR throttles them.
        // For example, if all 5 calls are blocked, you might see 5*(numRetries + 1) in worst case.
        assertTrue(totalRequests >= parallelInvocations,
                "Should be at least one request per initial invocation, plus retries.");
    }
}
