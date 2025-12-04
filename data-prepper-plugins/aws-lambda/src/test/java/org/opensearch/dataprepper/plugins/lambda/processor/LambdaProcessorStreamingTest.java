/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Mock PluginSetting
        when(pluginSetting.getName()).thenReturn("testStreamingProcessor");
        when(pluginSetting.getPipelineName()).thenReturn("testStreamingPipeline");
        
        // Mock PluginFactory to return JsonInputCodec
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class)))
                .thenReturn(new JsonInputCodec(new JsonInputCodecConfig()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"lambda-processor-streaming-config.yaml"})
    void testDoExecute_StreamingEnabled_UsesStreamingPath(String configFileName) {
        // Given
        LambdaProcessorConfig lambdaProcessorConfig = createLambdaConfigurationFromYaml(configFileName);
        LambdaProcessor lambdaProcessor = new LambdaProcessor(pluginFactory, pluginSetting, lambdaProcessorConfig, 
                awsCredentialsSupplier, expressionEvaluator, circuitBreaker);
        
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(Map.of("test", "streaming_data"))
                .build();
        Record<Event> testRecord = new Record<>(testEvent);
        Collection<Record<Event>> records = List.of(testRecord);

        // When
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Then
        assertNotNull(result);
        // Verify streaming configuration is loaded
        assertNotNull(lambdaProcessorConfig.getStreamingOptions());
        assertTrue(lambdaProcessorConfig.getStreamingOptions().isEnabled());
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
