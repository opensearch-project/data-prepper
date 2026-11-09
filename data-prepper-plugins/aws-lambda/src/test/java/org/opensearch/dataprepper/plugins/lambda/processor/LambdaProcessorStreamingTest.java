/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamResponseEvent;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamResponseHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.lambda.utils.LambdaTestSetupUtil.createLambdaConfigurationFromYaml;

class LambdaProcessorStreamingTest {

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private CircuitBreaker circuitBreaker;

    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // A fresh registry per test, so the processor's counters start at zero and record real counts.
        meterRegistry = new SimpleMeterRegistry();
        Metrics.addRegistry(meterRegistry);
        
        // Mock PluginSetting
        when(pluginSetting.getName()).thenReturn("testStreamingProcessor");
        when(pluginSetting.getPipelineName()).thenReturn("testStreamingPipeline");
        
        // Mock PluginFactory to return JsonInputCodec
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class)))
                .thenReturn(new JsonInputCodec(new JsonInputCodecConfig()));
    }

    @AfterEach
    void tearDown() {
        Metrics.removeRegistry(meterRegistry);
        meterRegistry.close();
    }

    @Test
    void testDoExecute_StreamingEnabled_UsesStreamingPath() {
        final LambdaAsyncClient lambdaAsyncClient = streamingClient(
                InvokeWithResponseStreamResponseEvent.payloadChunkBuilder()
                        .payload(SdkBytes.fromUtf8String("[{\"transformed\":\"yes\"}]"))
                        .build(),
                InvokeWithResponseStreamResponseEvent.invokeCompleteBuilder().build());

        final LambdaProcessor lambdaProcessor = streamingProcessor(lambdaAsyncClient);
        final List<Record<Event>> result = new ArrayList<>(doExecute(lambdaProcessor));

        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getData().get("transformed", String.class), equalTo("yes"));
        assertThat(lambdaProcessor.numberOfRequestsSuccessCounter.count(), equalTo(1.0));
        assertThat(lambdaProcessor.numberOfRequestsFailedCounter.count(), equalTo(0.0));
        // Each event must reach the function once, through the streaming call only.
        verify(lambdaAsyncClient).invokeWithResponseStream(any(InvokeWithResponseStreamRequest.class),
                any(InvokeWithResponseStreamResponseHandler.class));
        verify(lambdaAsyncClient, never()).invoke(any(InvokeRequest.class));
    }

    @Test
    void testDoExecute_StreamingFunctionError_TagsEventAsFailed() {
        final LambdaAsyncClient lambdaAsyncClient = streamingClient(
                InvokeWithResponseStreamResponseEvent.payloadChunkBuilder()
                        .payload(SdkBytes.fromUtf8String("{\"errorMessage\":\"Task timed out after 3.00 seconds\"}"))
                        .build(),
                InvokeWithResponseStreamResponseEvent.invokeCompleteBuilder()
                        .errorCode("Sandbox.Timedout")
                        .errorDetails("Task timed out after 3.00 seconds")
                        .build());

        assertEventTaggedAndCountedAsFailed(streamingProcessor(lambdaAsyncClient));
    }

    @Test
    void testDoExecute_StreamingCallFails_TagsEventAsFailed() {
        final LambdaAsyncClient lambdaAsyncClient = mock(LambdaAsyncClient.class);
        when(lambdaAsyncClient.invokeWithResponseStream(any(InvokeWithResponseStreamRequest.class),
                any(InvokeWithResponseStreamResponseHandler.class))).thenAnswer(invocation -> {
                    final InvokeWithResponseStreamResponseHandler handler = invocation.getArgument(1);
                    handler.exceptionOccurred(new RuntimeException("connection reset"));
                    return CompletableFuture.completedFuture(null);
                });

        assertEventTaggedAndCountedAsFailed(streamingProcessor(lambdaAsyncClient));
    }

    @Test
    void testDoExecute_StreamingCallInterrupted_TagsEventAndKeepsInterrupt() {
        final LambdaAsyncClient lambdaAsyncClient = mock(LambdaAsyncClient.class);
        // Never completes, so only the interrupt ends the wait in get().
        when(lambdaAsyncClient.invokeWithResponseStream(any(InvokeWithResponseStreamRequest.class),
                any(InvokeWithResponseStreamResponseHandler.class))).thenReturn(new CompletableFuture<>());
        final LambdaProcessor lambdaProcessor = streamingProcessor(lambdaAsyncClient);

        Thread.currentThread().interrupt();
        try {
            assertEventTaggedAndCountedAsFailed(lambdaProcessor);
            assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
        } finally {
            Thread.interrupted();
        }
    }

    /** The original event comes back tagged, so tags_on_failure routing can send it to a failure sink. */
    private void assertEventTaggedAndCountedAsFailed(final LambdaProcessor lambdaProcessor) {
        final List<Record<Event>> result = new ArrayList<>(doExecute(lambdaProcessor));

        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getData().get("test", String.class), equalTo("streaming_data"));
        assertThat(result.get(0).getData().getMetadata().getTags(), hasItem("lambda_streaming_failure"));
        assertThat(lambdaProcessor.numberOfRequestsFailedCounter.count(), equalTo(1.0));
        assertThat(lambdaProcessor.numberOfRecordsFailedCounter.count(), equalTo(1.0));
        assertThat(lambdaProcessor.numberOfRequestsSuccessCounter.count(), equalTo(0.0));
    }

    /** A client whose streamed call delivers the given events, then completes. */
    @SuppressWarnings("unchecked")
    private LambdaAsyncClient streamingClient(final InvokeWithResponseStreamResponseEvent... events) {
        final LambdaAsyncClient lambdaAsyncClient = mock(LambdaAsyncClient.class);
        when(lambdaAsyncClient.invokeWithResponseStream(any(InvokeWithResponseStreamRequest.class),
                any(InvokeWithResponseStreamResponseHandler.class))).thenAnswer(invocation -> {
                    final InvokeWithResponseStreamResponseHandler handler = invocation.getArgument(1);
                    final SdkPublisher<InvokeWithResponseStreamResponseEvent> publisher = mock(SdkPublisher.class);
                    when(publisher.subscribe(any(Consumer.class))).thenAnswer(subscribe -> {
                        final Consumer<InvokeWithResponseStreamResponseEvent> consumer = subscribe.getArgument(0);
                        for (final InvokeWithResponseStreamResponseEvent event : events) {
                            consumer.accept(event);
                        }
                        return CompletableFuture.completedFuture(null);
                    });
                    handler.onEventStream(publisher);
                    handler.complete();
                    return CompletableFuture.completedFuture(null);
                });
        return lambdaAsyncClient;
    }

    /** A streaming processor that uses the given client. */
    private LambdaProcessor streamingProcessor(final LambdaAsyncClient lambdaAsyncClient) {
        final LambdaProcessorConfig lambdaProcessorConfig =
                createLambdaConfigurationFromYaml("lambda-processor-streaming-config.yaml");
        try (MockedStatic<LambdaClientFactory> clientFactory = mockStatic(LambdaClientFactory.class)) {
            clientFactory.when(() -> LambdaClientFactory.createAsyncLambdaClient(any(), any(), any()))
                    .thenReturn(lambdaAsyncClient);
            return new LambdaProcessor(pluginFactory, pluginSetting, lambdaProcessorConfig, awsCredentialsSupplier,
                    expressionEvaluator, circuitBreaker);
        }
    }

    /** Runs one event through the processor. */
    private Collection<Record<Event>> doExecute(final LambdaProcessor lambdaProcessor) {
        final Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(Map.of("test", "streaming_data"))
                .build();
        return lambdaProcessor.doExecute(List.of(new Record<>(testEvent)));
    }

    @Test
    void testDoExecute_EmptyRecords_ReturnsEmpty() {
        // Given
        LambdaProcessorConfig lambdaProcessorConfig = createLambdaConfigurationFromYaml("lambda-processor-streaming-config.yaml");
        LambdaProcessor lambdaProcessor = new LambdaProcessor(pluginFactory, pluginSetting, lambdaProcessorConfig, 
                awsCredentialsSupplier, expressionEvaluator, circuitBreaker);
        
        Collection<Record<Event>> emptyRecords = Collections.emptyList();

        // When
        Collection<Record<Event>> result = lambdaProcessor.doExecute(emptyRecords);

        // Then
        assertEquals(emptyRecords, result);
    }
}
