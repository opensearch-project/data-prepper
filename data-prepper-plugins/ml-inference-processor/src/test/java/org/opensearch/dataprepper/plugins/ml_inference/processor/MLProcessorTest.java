/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.common.MLBatchJobCreator;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ServiceName;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.NUMBER_OF_ML_PROCESSOR_FAILED;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.NUMBER_OF_ML_PROCESSOR_SUCCESS;

@ExtendWith(MockitoExtension.class)
public class MLProcessorTest {
    @Mock
    private MLProcessorConfig mlProcessorConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private MLBatchJobCreator mlBatchJobCreator;

    @Mock
    private Counter successCounter;

    @Mock
    private Counter failureCounter;

    @Mock
    private MLProcessor mlProcessor;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        MockitoAnnotations.openMocks(this);
        when(mlProcessorConfig.getWhenCondition()).thenReturn("condition");
        lenient().when(expressionEvaluator.evaluateConditional(eq("condition"), any())).thenReturn(true);
        lenient().when(mlProcessorConfig.getServiceName()).thenReturn(ServiceName.SAGEMAKER);
        lenient().when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_WEST_2);
        lenient().when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        lenient().when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        lenient().when(pluginMetrics.counter(NUMBER_OF_ML_PROCESSOR_SUCCESS)).thenReturn(successCounter);
        lenient().when(pluginMetrics.counter(NUMBER_OF_ML_PROCESSOR_FAILED)).thenReturn(failureCounter);

        mlProcessor = new MLProcessor(mlProcessorConfig, pluginMetrics, awsCredentialsSupplier, expressionEvaluator);
        // Inject the mocked mlBatchJobCreator using reflection
        Field field = MLProcessor.class.getDeclaredField("mlBatchJobCreator");
        field.setAccessible(true);
        field.set(mlProcessor, mlBatchJobCreator);
    }

    @Test
    void testDoExecute_WithValidRecords() throws Exception {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);

        Collection<Record<Event>> result = mlProcessor.doExecute(records);

        verify(mlBatchJobCreator, times(1)).createMLBatchJob(records, new ArrayList<>());
        verify(successCounter, times(1)).increment();
    }

    @Test
    void testDoExecute_WithNoRecords() {
        Collection<Record<Event>> result = mlProcessor.doExecute(Collections.emptyList());

        verifyNoInteractions(mlBatchJobCreator, successCounter, failureCounter);
        assertTrue(result.isEmpty());
    }

    @Test
    void testDoExecute_WithConditionNotMet() {
        // Mock event and record
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);

        // Mock the expression evaluator
        when(expressionEvaluator.evaluateConditional(eq("condition"), any())).thenReturn(false);

        // Ensure no interaction with mlBatchJobCreator
        Collection<Record<Event>> result = mlProcessor.doExecute(records);

        // Verify no interactions with mlBatchJobCreator, successCounter, or failureCounter
        verifyNoInteractions(mlBatchJobCreator, successCounter, failureCounter);

        // Assert that the input records are returned as output
        assertEquals(records, result);
    }

    @Test
    void testDoExecute_WithException() throws Exception {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);

        doThrow(new RuntimeException("Test Exception")).when(mlBatchJobCreator).createMLBatchJob(records, new ArrayList<>());

        Collection<Record<Event>> result = mlProcessor.doExecute(records);

        verify(failureCounter, times(1)).increment();
    }

    @Test
    void testShutdownMethods() {
        assertTrue(mlProcessor.isReadyForShutdown());
        mlProcessor.prepareForShutdown();
        mlProcessor.shutdown();
    }
}
