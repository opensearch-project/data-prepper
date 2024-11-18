/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.processor;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodecConfig;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LambdaProcessorIT {
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
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private Counter testCounter;
    @Mock
    private Timer testTimer;
    @Mock
    InvocationType invocationType;
    private LambdaProcessor createObjectUnderTest(LambdaProcessorConfig processorConfig) {
        return new LambdaProcessor(pluginFactory, pluginMetrics, processorConfig, awsCredentialsSupplier, expressionEvaluator);
    }

    @BeforeEach
    public void setup() {
        lambdaRegion = System.getProperty("tests.lambda.processor.region");
        functionName = System.getProperty("tests.lambda.processor.functionName");
        role = System.getProperty("tests.lambda.processor.sts_role_arn");
        pluginMetrics = mock(PluginMetrics.class);
        //when(pluginMetrics.gauge(any(), any(AtomicLong.class))).thenReturn(new AtomicLong());
        //testCounter = mock(Counter.class);
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(testCounter).increment(any(Double.class));
        } catch (Exception e){}
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(testTimer).record(any(Long.class), any(TimeUnit.class));
        } catch (Exception e){}
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
        validateResultsForAggregateMode(results );
    }

    @ParameterizedTest
    @ValueSource(ints = {1000})
    public void testRequestResponse_WithMatchingEvents_StrictMode_WithMultipleThreads(int numRecords) throws InterruptedException {
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
    public void testDifferentInvocationTypes(String invocationType) {
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
            assertThat(results.size(), equalTo(0));
        }
    }

    @Test
    public void testWithFailureTags() {
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
            int id = (Integer)eventData.get("id");
            assertThat(eventData.get("key"+id), equalTo(id));
            String stringValue = "value"+id;
            assertThat(eventData.get("keys"+id), equalTo(stringValue.toUpperCase()));
            assertThat(attr.get("attr"+id), equalTo(id));
            assertThat(attr.get("attrs"+id), equalTo("attrvalue"+id));
        }
    }

    private List<Record<Event>> createRecords(int numRecords) {
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", i);
            map.put("key"+i, i);
            map.put("keys"+i, "value"+i);
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("attr"+i, i);
            attrs.put("attrs"+i, "attrvalue"+i);
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
}
