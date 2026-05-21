/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.BatchActionExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.MLActionExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.ModelSyncInferenceExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.PredictActionExecutor;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ServiceName;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.NUMBER_OF_ML_PROCESSOR_FAILED;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.NUMBER_OF_ML_PROCESSOR_SUCCESS;

@ExtendWith(MockitoExtension.class)
public class MLProcessorTest {

    @Mock private MLProcessorConfig mlProcessorConfig;
    @Mock private PluginMetrics pluginMetrics;
    @Mock private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock private ExpressionEvaluator expressionEvaluator;
    @Mock private Counter successCounter;
    @Mock private Counter failureCounter;
    @Mock private AwsAuthenticationOptions awsAuthenticationOptions;
    @Mock private AwsCredentialsProvider awsCredentialsProvider;
    @Mock private PluginFactory pluginFactory;
    @Mock private PluginSetting pluginSetting;

    private void setupCommonMocks() {
        MockitoAnnotations.openMocks(this);
        lenient().when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_WEST_2);
        lenient().when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        lenient().when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        lenient().when(mlProcessorConfig.getDlqPluginSetting()).thenReturn(null);
        lenient().when(pluginMetrics.counter(NUMBER_OF_ML_PROCESSOR_SUCCESS)).thenReturn(successCounter);
        lenient().when(pluginMetrics.counter(NUMBER_OF_ML_PROCESSOR_FAILED)).thenReturn(failureCounter);
        lenient().when(pluginMetrics.counter(ModelSyncInferenceExecutor.NUMBER_OF_SYNC_INFERENCE_RECORDS_SUCCESS)).thenReturn(mock(io.micrometer.core.instrument.Counter.class));
        lenient().when(pluginMetrics.counter(ModelSyncInferenceExecutor.NUMBER_OF_SYNC_INFERENCE_RECORDS_FAILED)).thenReturn(mock(io.micrometer.core.instrument.Counter.class));
    }

    private MLProcessor buildProcessor(final ActionType actionType) throws Exception {
        setupCommonMocks();
        when(mlProcessorConfig.getWhenCondition()).thenReturn("condition");
        lenient().when(expressionEvaluator.evaluateConditional(eq("condition"), any())).thenReturn(true);
        lenient().when(mlProcessorConfig.getActionType()).thenReturn(actionType);

        if (ActionType.PREDICT.equals(actionType)) {
            lenient().when(mlProcessorConfig.getModelId()).thenReturn("amazon.titan-embed-text-v2:0");
            lenient().when(mlProcessorConfig.getTagsOnFailure()).thenReturn(Collections.emptyList());
        } else {
            lenient().when(mlProcessorConfig.getServiceName()).thenReturn(ServiceName.SAGEMAKER);
        }
        return new MLProcessor(mlProcessorConfig, pluginMetrics, pluginFactory, pluginSetting, awsCredentialsSupplier, expressionEvaluator);
    }

    private void injectExecutor(final MLProcessor processor, final MLActionExecutor executor) throws Exception {
        final Field field = MLProcessor.class.getDeclaredField("actionExecutor");
        field.setAccessible(true);
        field.set(processor, executor);
    }

    @Nested
    class BatchPredictMode {

        private MLProcessor mlProcessor;
        private BatchActionExecutor batchActionExecutor;

        @BeforeEach
        void setUp() throws Exception {
            batchActionExecutor = mock(BatchActionExecutor.class);
            mlProcessor = buildProcessor(ActionType.BATCH_PREDICT);
            injectExecutor(mlProcessor, batchActionExecutor);
        }

        @Test
        void testDoExecute_WithValidRecords() {
            final Event event = mock(Event.class);
            final Record<Event> record = new Record<>(event);
            final List<Record<Event>> records = Collections.singletonList(record);

            mlProcessor.doExecute(records);

            verify(batchActionExecutor, times(1)).prepareExecution(any());
            verify(batchActionExecutor, times(1)).execute(eq(records), any());
            verify(successCounter, times(1)).increment();
        }

        @Test
        void testDoExecute_WithNoRecords() {
            final Collection<Record<Event>> result = mlProcessor.doExecute(Collections.emptyList());

            verify(batchActionExecutor, times(1)).prepareExecution(any());
            verify(batchActionExecutor, never()).execute(any(), any());
            verifyNoInteractions(successCounter, failureCounter);
            assertTrue(result.isEmpty());
        }

        @Test
        void testDoExecute_WithConditionNotMet() {
            final Event event = mock(Event.class);
            final Record<Event> record = new Record<>(event);
            final List<Record<Event>> records = Collections.singletonList(record);
            when(expressionEvaluator.evaluateConditional(eq("condition"), any())).thenReturn(false);

            final Collection<Record<Event>> result = mlProcessor.doExecute(records);

            verify(batchActionExecutor, times(1)).prepareExecution(any());
            verify(batchActionExecutor, never()).execute(any(), any());
            verifyNoInteractions(successCounter, failureCounter);
            assertEquals(records, result);
        }

        @Test
        void testDoExecute_WithException() {
            final Event event = mock(Event.class);
            final Record<Event> record = new Record<>(event);
            final List<Record<Event>> records = Collections.singletonList(record);
            doThrow(new RuntimeException("Test Exception")).when(batchActionExecutor).execute(any(), any());

            mlProcessor.doExecute(records);

            verify(failureCounter, times(1)).increment();
        }

        @Test
        void testShutdownMethods() {
            when(batchActionExecutor.isReadyForShutdown()).thenReturn(true);

            assertTrue(mlProcessor.isReadyForShutdown());
            mlProcessor.prepareForShutdown();
            mlProcessor.shutdown();

            verify(batchActionExecutor).isReadyForShutdown();
            verify(batchActionExecutor).prepareForShutdown();
            verify(batchActionExecutor).shutdown();
        }
    }

    @Nested
    class PredictMode {

        private MLProcessor mlProcessor;
        private PredictActionExecutor predictActionExecutor;

        @BeforeEach
        void setUp() throws Exception {
            predictActionExecutor = mock(PredictActionExecutor.class);
            mlProcessor = buildProcessor(ActionType.PREDICT);
            injectExecutor(mlProcessor, predictActionExecutor);
        }

        @Test
        @SuppressWarnings("unchecked")
        void testDoExecute_WithValidRecords_delegatesToExecutor() {
            final Event event = mock(Event.class);
            final Record<Event> record = new Record<>(event);
            final List<Record<Event>> records = Collections.singletonList(record);
            doAnswer(invocation -> {
                final List<Record<Event>> result = invocation.getArgument(1);
                result.addAll(records);
                return result;
            }).when(predictActionExecutor).execute(eq(records), any());

            final Collection<Record<Event>> result = mlProcessor.doExecute(records);

            verify(predictActionExecutor, times(1)).execute(eq(records), any());
            verify(successCounter, times(1)).increment();
            assertEquals(records, new java.util.ArrayList<>(result));
        }

        @Test
        void testDoExecute_WithNoRecords_returnsEmpty() {
            final Collection<Record<Event>> result = mlProcessor.doExecute(Collections.emptyList());

            verify(predictActionExecutor, never()).execute(any(), any());
            verifyNoInteractions(successCounter, failureCounter);
            assertTrue(result.isEmpty());
        }

        @Test
        void testDoExecute_WithConditionNotMet_skipsExecutor() {
            final Event event = mock(Event.class);
            final Record<Event> record = new Record<>(event);
            final List<Record<Event>> records = Collections.singletonList(record);
            when(expressionEvaluator.evaluateConditional(eq("condition"), any())).thenReturn(false);

            final Collection<Record<Event>> result = mlProcessor.doExecute(records);

            verify(predictActionExecutor, never()).execute(any(), any());
            verifyNoInteractions(successCounter, failureCounter);
            assertEquals(records, result);
        }

        @Test
        void testDoExecute_WithConditionMet_passesFilteredRecordsToExecutor() {
            final Event matchedEvent = mock(Event.class);
            final Event skippedEvent = mock(Event.class);
            final Record<Event> matchedRecord = new Record<>(matchedEvent);
            final Record<Event> skippedRecord = new Record<>(skippedEvent);
            final List<Record<Event>> records = List.of(matchedRecord, skippedRecord);

            when(expressionEvaluator.evaluateConditional(eq("condition"), eq(matchedEvent))).thenReturn(true);
            when(expressionEvaluator.evaluateConditional(eq("condition"), eq(skippedEvent))).thenReturn(false);

            final List<Record<Event>> filteredRecords = Collections.singletonList(matchedRecord);
            doAnswer(invocation -> {
                final List<Record<Event>> result = invocation.getArgument(1);
                result.addAll(filteredRecords);
                return result;
            }).when(predictActionExecutor).execute(eq(filteredRecords), any());

            final Collection<Record<Event>> result = mlProcessor.doExecute(records);

            verify(predictActionExecutor, times(1)).execute(eq(filteredRecords), any());
            assertTrue(result.contains(matchedRecord));
            assertTrue(result.contains(skippedRecord));
            assertEquals(2, result.size());
        }

        @Test
        void testDoExecute_ExecutorThrows_incrementsFailureCounter() {
            final Event event = mock(Event.class);
            final Record<Event> record = new Record<>(event);
            final List<Record<Event>> records = Collections.singletonList(record);
            doThrow(new RuntimeException("predict failed")).when(predictActionExecutor).execute(any(), any());

            mlProcessor.doExecute(records);

            verify(failureCounter, times(1)).increment();
            verifyNoInteractions(successCounter);
        }

        @Test
        void testShutdownMethods_areNoOps() {
            when(predictActionExecutor.isReadyForShutdown()).thenReturn(true);

            assertTrue(mlProcessor.isReadyForShutdown());
            mlProcessor.prepareForShutdown();
            mlProcessor.shutdown();

            verify(predictActionExecutor).isReadyForShutdown();
            verify(predictActionExecutor).prepareForShutdown();
            verify(predictActionExecutor).shutdown();
        }
    }
}
