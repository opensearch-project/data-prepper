/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.RemoteConnectorExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.ModelSyncInferenceExecutor.NUMBER_OF_SYNC_INFERENCE_RECORDS_FAILED;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.ModelSyncInferenceExecutor.NUMBER_OF_SYNC_INFERENCE_RECORDS_SUCCESS;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ModelSyncInferenceExecutorTest {

    @Mock private MLProcessorConfig config;
    @Mock private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock private AwsAuthenticationOptions awsAuthOptions;
    @Mock private RemoteConnectorExecutor connectorExecutor;
    @Mock private Event event;
    @Mock private PluginMetrics pluginMetrics;
    @Mock private Counter successCounter;
    @Mock private Counter failureCounter;

    private ModelSyncInferenceExecutor predictProcessor;

    @BeforeEach
    void setUp() throws Exception {
        when(config.getModelId()).thenReturn("amazon.titan-embed-text-v2:0");
        when(config.getAwsAuthenticationOptions()).thenReturn(awsAuthOptions);
        when(awsAuthOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(config.getTagsOnFailure()).thenReturn(Collections.emptyList());
        when(config.getInputMap()).thenReturn(List.of(Map.of("inputText", "passage_text")));
        when(config.getOutputMap()).thenReturn(List.of(Map.of("passage_embedding", "embedding")));
        when(pluginMetrics.counter(NUMBER_OF_SYNC_INFERENCE_RECORDS_SUCCESS)).thenReturn(successCounter);
        when(pluginMetrics.counter(NUMBER_OF_SYNC_INFERENCE_RECORDS_FAILED)).thenReturn(failureCounter);

        predictProcessor = new ModelSyncInferenceExecutor(config, awsCredentialsSupplier, pluginMetrics);
        injectConnectorExecutor(predictProcessor, connectorExecutor);
    }

    private ModelSyncInferenceExecutor buildAndInject() throws Exception {
        final ModelSyncInferenceExecutor executor = new ModelSyncInferenceExecutor(config, awsCredentialsSupplier, pluginMetrics);
        injectConnectorExecutor(executor, connectorExecutor);
        return executor;
    }

    @Test
    void execute_success_writesEmbeddingToEvent() {
        when(event.get("passage_text", String.class)).thenReturn("hello world");
        when(connectorExecutor.executeActionAndGetResponse(eq(ConnectorActionType.PREDICT), any()))
                .thenReturn("{\"embedding\":[0.1,0.2,0.3]}");

        final Collection<Record<Event>> results = predictProcessor.execute(List.of(new Record<>(event)));

        assertThat(results, hasSize(1));
        verify(event).put(eq("passage_embedding"), any());
        verify(successCounter, times(1)).increment();
        verify(failureCounter, never()).increment();
    }

    @Test
    void execute_multipleInputMappings_invokesModelForEachMapping() throws Exception {
        when(config.getInputMap()).thenReturn(List.of(
                Map.of("inputText", "field_a"),
                Map.of("inputText", "field_b")
        ));
        when(config.getOutputMap()).thenReturn(List.of(
                Map.of("embedding_a", "/embedding"),
                Map.of("embedding_b", "/embedding")
        ));
        when(event.get("field_a", String.class)).thenReturn("text a");
        when(event.get("field_b", String.class)).thenReturn("text b");
        when(connectorExecutor.executeActionAndGetResponse(eq(ConnectorActionType.PREDICT), any()))
                .thenReturn("{\"embedding\":[0.1,0.2]}");

        predictProcessor = buildAndInject();

        final Collection<Record<Event>> results = predictProcessor.execute(List.of(new Record<>(event)));

        assertThat(results, hasSize(1));
        verify(event).put(eq("embedding_a"), any());
        verify(event).put(eq("embedding_b"), any());
        verify(successCounter, times(1)).increment();
    }

    @Test
    void execute_missingInputField_incrementsFailureCounterAndReturnsRecord() throws Exception {
        when(config.getTagsOnFailure()).thenReturn(List.of("_ml_inference_failure"));
        when(event.get("passage_text", String.class)).thenReturn(null);
        when(event.getMetadata()).thenReturn(null);

        predictProcessor = buildAndInject();

        final Collection<Record<Event>> results = predictProcessor.execute(List.of(new Record<>(event)));

        assertThat(results, hasSize(1));
        verify(connectorExecutor, never()).executeActionAndGetResponse(any(), any());
        verify(failureCounter, times(1)).increment();
        verify(successCounter, never()).increment();
    }

    @Test
    void execute_modelReturnsNestedOutputPath_extractsCorrectly() throws Exception {
        when(config.getOutputMap()).thenReturn(List.of(Map.of("passage_embedding", "modelOutput/embedding")));
        when(event.get("passage_text", String.class)).thenReturn("hello");
        when(connectorExecutor.executeActionAndGetResponse(eq(ConnectorActionType.PREDICT), any()))
                .thenReturn("{\"modelOutput\":{\"embedding\":[0.5,0.6]}}");

        predictProcessor = buildAndInject();

        final Collection<Record<Event>> results = predictProcessor.execute(List.of(new Record<>(event)));

        assertThat(results, hasSize(1));
        verify(event).put(eq("passage_embedding"), any());
        verify(successCounter, times(1)).increment();
    }

    @Test
    void execute_remoteServiceThrows_incrementsFailureCounterAndReturnsRecord() throws Exception {
        when(config.getTagsOnFailure()).thenReturn(List.of("_ml_failure"));
        when(event.get("passage_text", String.class)).thenReturn("hello");
        when(event.getMetadata()).thenReturn(null);
        when(connectorExecutor.executeActionAndGetResponse(any(), any()))
                .thenThrow(new MLBatchJobException(500, "server error"));

        predictProcessor = buildAndInject();

        final Collection<Record<Event>> results = predictProcessor.execute(List.of(new Record<>(event)));

        assertThat(results, hasSize(1));
        verify(failureCounter, times(1)).increment();
        verify(successCounter, never()).increment();
    }

    @Test
    void execute_emptyRecords_returnsEmpty() {
        final Collection<Record<Event>> results = predictProcessor.execute(Collections.emptyList());

        assertThat(results, hasSize(0));
        verify(successCounter, never()).increment();
        verify(failureCounter, never()).increment();
    }

    @Test
    void execute_outputPathNotFound_incrementsFailureCounterAndReturnsRecord() {
        when(event.get("passage_text", String.class)).thenReturn("hello");
        when(event.getMetadata()).thenReturn(null);
        when(connectorExecutor.executeActionAndGetResponse(any(), any()))
                .thenReturn("{\"other_field\":\"value\"}");

        final Collection<Record<Event>> results = predictProcessor.execute(List.of(new Record<>(event)));

        assertThat(results, hasSize(1));
        verify(failureCounter, times(1)).increment();
        verify(successCounter, never()).increment();
    }

    @Test
    void execute_noOutputMap_doesNotWriteToEvent() throws Exception {
        when(config.getOutputMap()).thenReturn(Collections.emptyList());
        when(event.get("passage_text", String.class)).thenReturn("hello");
        when(connectorExecutor.executeActionAndGetResponse(any(), any()))
                .thenReturn("{\"embedding\":[0.1]}");

        predictProcessor = buildAndInject();

        final Collection<Record<Event>> results = predictProcessor.execute(List.of(new Record<>(event)));

        assertThat(results, hasSize(1));
        verify(event, never()).put(any(String.class), any());
        verify(successCounter, times(1)).increment();
    }

    @Test
    void execute_multipleRecords_countsEachIndependently() throws Exception {
        final Event successEvent = event;
        final Event failureEvent = org.mockito.Mockito.mock(Event.class);
        when(successEvent.get("passage_text", String.class)).thenReturn("hello");
        when(failureEvent.get("passage_text", String.class)).thenReturn(null);
        when(failureEvent.getMetadata()).thenReturn(null);
        when(connectorExecutor.executeActionAndGetResponse(any(), any()))
                .thenReturn("{\"embedding\":[0.1]}");

        final Collection<Record<Event>> results = predictProcessor.execute(
                List.of(new Record<>(successEvent), new Record<>(failureEvent)));

        assertThat(results, hasSize(2));
        verify(successCounter, times(1)).increment();
        verify(failureCounter, times(1)).increment();
    }

    @Test
    void execute_passesRegionInParameters() {
        when(event.get("passage_text", String.class)).thenReturn("hello");
        when(connectorExecutor.executeActionAndGetResponse(eq(ConnectorActionType.PREDICT), any()))
                .thenReturn("{\"embedding\":[0.1]}");

        predictProcessor.execute(List.of(new Record<>(event)));

        verify(connectorExecutor).executeActionAndGetResponse(
                eq(ConnectorActionType.PREDICT),
                argThatContainsRegion("us-east-1")
        );
    }

    // --- helpers ---

    private Map<String, String> argThatContainsRegion(final String region) {
        return org.mockito.ArgumentMatchers.argThat(
                params -> params != null && region.equals(params.get("region")));
    }

    private void injectConnectorExecutor(final ModelSyncInferenceExecutor processor,
                                          final RemoteConnectorExecutor executor) {
        try {
            final Field field = ModelSyncInferenceExecutor.class.getDeclaredField("connectorExecutor");
            field.setAccessible(true);
            field.set(processor, executor);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
