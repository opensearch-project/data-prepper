/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda;

import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.types.ByteCount;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor;
import org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessorConfig;
import org.opensearch.dataprepper.plugins.lambda.sink.LambdaSink;
import org.opensearch.dataprepper.plugins.lambda.sink.LambdaSinkConfig;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayList;
import java.util.Collection;
import java.lang.reflect.Field;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LambdaProcessorSinkIT {
    private AwsCredentialsProvider awsCredentialsProvider;
    private LambdaProcessor lambdaProcessor;
    private LambdaProcessorConfig lambdaProcessorConfig;
    private String functionName;
    private String lambdaRegion;
    private String role;

    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private LambdaSinkConfig lambdaSinkConfig;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private Counter numberOfRecordsSuccessCounter;
    @Mock
    private Counter numberOfRecordsFailedCounter;
    @Mock
    private Counter numberOfRequestsSuccessCounter;
    @Mock
    private Counter numberOfRequestsFailedCounter;
    @Mock
    private Counter sinkSuccessCounter;
    @Mock
    private Timer lambdaLatencyMetric;
    @Mock
    private DistributionSummary requestPayloadMetric;
    @Mock
    private DistributionSummary responsePayloadMetric;
    @Mock
    InvocationType invocationType;
    @Mock
    CircuitBreaker circuitBreaker;

    private AtomicLong successCount;
    private AtomicLong numEventHandlesReleased;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    private LambdaProcessor createLambdaProcessor(LambdaProcessorConfig processorConfig) {
        return new LambdaProcessor(pluginFactory, pluginSetting, processorConfig,
                awsCredentialsSupplier, expressionEvaluator,
                circuitBreaker);
    }

    private LambdaSink createLambdaSink(LambdaSinkConfig lambdaSinkConfig) {
        return new LambdaSink(pluginSetting, lambdaSinkConfig, pluginFactory, null, awsCredentialsSupplier, expressionEvaluator);

    }

    private void setPrivateField(Object targetObject, String fieldName, Object value) throws Exception {
        Field field = targetObject.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(targetObject, value);
    }

    @BeforeEach
    public void setup() {
        lambdaRegion = System.getProperty("tests.lambda.processor.region");
        functionName = System.getProperty("tests.lambda.processor.functionName");
        role = System.getProperty("tests.lambda.processor.sts_role_arn");
        successCount = new AtomicLong();
        numEventHandlesReleased = new AtomicLong();
        numberOfRecordsSuccessCounter = mock(Counter.class);
        numberOfRecordsFailedCounter = mock(Counter.class);
        numberOfRequestsSuccessCounter = mock(Counter.class);
        numberOfRequestsFailedCounter = mock(Counter.class);
        lambdaLatencyMetric = mock(Timer.class);
        requestPayloadMetric = mock(DistributionSummary.class);
        responsePayloadMetric = mock(DistributionSummary.class);

        acknowledgementSet = mock(AcknowledgementSet.class);
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(acknowledgementSet).acquire(any(EventHandle.class));
        } catch (Exception e){ }
        try {
            lenient().doAnswer(args -> {
                numEventHandlesReleased.incrementAndGet();
                return null;
            }).when(acknowledgementSet).release(any(EventHandle.class), any(Boolean.class));
        } catch (Exception e){ }
        pluginMetrics = mock(PluginMetrics.class);
        sinkSuccessCounter = mock(Counter.class);
        try {
            lenient().doAnswer(args -> {
                Double c = args.getArgument(0);
                successCount.addAndGet(c.intValue());
                return null;
            }).when(sinkSuccessCounter).increment(any(Double.class));
        } catch (Exception e){ }
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(numberOfRecordsSuccessCounter).increment(any(Double.class));
        } catch (Exception e){}
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(numberOfRecordsFailedCounter).increment();
        } catch (Exception e){}
        try {
            lenient().doAnswer(args -> {
                return null;
            }).when(lambdaLatencyMetric).record(any(Long.class), any(TimeUnit.class));
        } catch (Exception e){}

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
        //when(lambdaProcessorConfig.getMaxConnectionRetries()).thenReturn(3);
        BatchOptions batchOptions = mock(BatchOptions.class);
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);
        when(lambdaProcessorConfig.getTagsOnFailure()).thenReturn(null);
        invocationType = mock(InvocationType.class);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(invocationType);
        when(lambdaProcessorConfig.getResponseCodecConfig()).thenReturn(null);
        //when(lambdaProcessorConfig.getConnectionTimeout()).thenReturn(DEFAULT_CONNECTION_TIMEOUT);
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

        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(pluginSetting.getName()).thenReturn("name");
        lambdaSinkConfig = mock(LambdaSinkConfig.class);
        when(lambdaSinkConfig.getFunctionName()).thenReturn(functionName);
        when(lambdaSinkConfig.getDlqPluginSetting()).thenReturn(null);

        InvocationType sinkInvocationType = mock(InvocationType.class);
        when(sinkInvocationType.getAwsLambdaValue()).thenReturn(InvocationType.EVENT.getAwsLambdaValue());
        when(lambdaSinkConfig.getInvocationType()).thenReturn(invocationType);
        when(lambdaSinkConfig.getBatchOptions()).thenReturn(batchOptions);
        when(lambdaSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);

    }

    private void setPrivateFields(final LambdaProcessor lambdaProcessor) throws Exception {
        setPrivateField(lambdaProcessor, "numberOfRecordsSuccessCounter", numberOfRecordsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
        setPrivateField(lambdaProcessor, "numberOfRequestsSuccessCounter", numberOfRequestsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRequestsFailedCounter", numberOfRequestsFailedCounter);
        setPrivateField(lambdaProcessor, "lambdaLatencyMetric", lambdaLatencyMetric);
        setPrivateField(lambdaProcessor, "requestPayloadMetric", requestPayloadMetric);
        setPrivateField(lambdaProcessor, "responsePayloadMetric", responsePayloadMetric);
    }

    @ParameterizedTest
    @ValueSource(ints = {11})
    public void testLambdaProcessorAndLambdaSink(int numRecords) throws Exception {
        when(invocationType.getAwsLambdaValue()).thenReturn(InvocationType.REQUEST_RESPONSE.getAwsLambdaValue());
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true);
        lambdaProcessor = createLambdaProcessor(lambdaProcessorConfig);
        setPrivateFields(lambdaProcessor);
        List<Record<Event>> records = createRecords(numRecords);

        Collection<Record<Event>> results = lambdaProcessor.doExecute(records);

        assertThat(results.size(), equalTo(numRecords));
        validateStrictModeResults(records, results);
        LambdaSink lambdaSink = createLambdaSink(lambdaSinkConfig);
        setPrivateField(lambdaSink, "numberOfRecordsSuccessCounter", sinkSuccessCounter);
        lambdaSink.output(results);
        assertThat(successCount.get(), equalTo((long)numRecords));
        assertThat(numEventHandlesReleased.get(), equalTo((long)numRecords));
    }

    private void validateResultsForAggregateMode(List<Record<Event>> records, Collection<Record<Event>> results) {
        List<Record<Event>> resultRecords = new ArrayList<>(results);
        Map<Integer, EventHandle> eventHandlesMap = new HashMap<>();
        for (final Record<Event> record: records) {
            eventHandlesMap.put((Integer)record.getData().toMap().get("id"), record.getData().getEventHandle());
        }
        for (int i = 0; i < resultRecords.size(); i++) {
            Event event = resultRecords.get(i).getData();
            Map<String, Object> eventData = event.toMap();

            // Check if the event contains the expected data
            assertThat(eventData.containsKey("id"), equalTo(true));
            int id = (Integer) eventData.get("id");
            assertThat(eventData.get("key" + id), equalTo(id));
            String stringValue = "value" + id;
            assertThat(eventData.get("keys" + id), equalTo(stringValue.toUpperCase()));
            assertThat(event.getEventHandle(), not(equalTo(eventHandlesMap.get(id))));

            // Check that there's no metadata or it's empty
            EventMetadata metadata = event.getMetadata();
            if (metadata != null) {
                assertThat(metadata.getAttributes().isEmpty(), equalTo(true));
                assertThat(metadata.getTags().isEmpty(), equalTo(true));
            }
        }
    }

    private void validateStrictModeResults(List<Record<Event>> records, Collection<Record<Event>> results) {
        List<Record<Event>> resultRecords = new ArrayList<>(results);
        Map<Integer, EventHandle> eventHandlesMap = new HashMap<>();
        for (final Record<Event> record: records) {
            eventHandlesMap.put((Integer)record.getData().toMap().get("id"), record.getData().getEventHandle());
        }
        for (int i = 0; i < resultRecords.size(); i++) {
            Event event = resultRecords.get(i).getData();
            Map<String, Object> eventData = event.toMap();
            Map<String, Object> attr = event.getMetadata().getAttributes();
            int id = (Integer)eventData.get("id");
            assertThat(event.getEventHandle(), equalTo(eventHandlesMap.get(id)));
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
            ((DefaultEventHandle)event.getEventHandle()).addAcknowledgementSet(acknowledgementSet);
            records.add(new Record<>(event));
        }
        return records;
    }

}
