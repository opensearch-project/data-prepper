/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.ion;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.processor.parse.AbstractParseProcessor;
import org.opensearch.dataprepper.plugins.processor.parse.json.ParseJsonProcessorTest;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParseIonProcessorTest extends ParseJsonProcessorTest {
    @Mock
    private ParseIonProcessorConfig ionProcessorConfig;

    @BeforeEach
    @Override
    public void setup() {
        processorConfig = ionProcessorConfig;
        ParseIonProcessorConfig defaultConfig = new ParseIonProcessorConfig();
        when(processorConfig.getSource()).thenReturn(defaultConfig.getSource());
        when(processorConfig.getDestination()).thenReturn(defaultConfig.getDestination());
        when(processorConfig.getPointer()).thenReturn(defaultConfig.getPointer());
        when(processorConfig.getParseWhen()).thenReturn(null);
        when(processorConfig.getOverwriteIfDestinationExists()).thenReturn(true);

        when(pluginMetrics.counter("recordsIn")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("recordsOut")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("processingFailures")).thenReturn(this.processingFailuresCounter);
        when(pluginMetrics.counter("parseErrors")).thenReturn(this.parseErrorsCounter);
        when(processorConfig.getHandleFailedEventsOption()).thenReturn(handleFailedEventsOption);
    }

    @Override
    protected AbstractParseProcessor createObjectUnderTest() {
        return new ParseIonProcessor(pluginMetrics, ionProcessorConfig, expressionEvaluator, testEventKeyFactory);
    }

    @Test
    void invalid_parse_when_throws_InvalidPluginConfigurationException() {
        final String parseWhen = UUID.randomUUID().toString();

        when(processorConfig.getParseWhen()).thenReturn(parseWhen);
        when(expressionEvaluator.isValidExpressionStatement(parseWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void test_when_using_ion_features_then_processorParsesCorrectly() {
        parseJsonProcessor = createObjectUnderTest();

        final String serializedMessage = "{bareKey: 1, symbol: SYMBOL, timestamp: 2023-11-30T21:05:23.383Z, attribute: dollars::100.0 }";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(true));
        assertThat(parsedEvent.get(processorConfig.getSource(), Object.class), equalTo(serializedMessage));
        assertThat(parsedEvent.get("bareKey", Integer.class), equalTo(1));
        assertThat(parsedEvent.get("symbol", String.class), equalTo("SYMBOL"));
        assertThat(parsedEvent.get("timestamp", String.class), equalTo("2023-11-30T21:05:23.383Z"));
        assertThat(parsedEvent.get("attribute", Double.class), equalTo(100.0));

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_when_deleteSourceFlagEnabled() {
        when(processorConfig.isDeleteSourceRequested()).thenReturn(true);
        parseJsonProcessor = createObjectUnderTest();

        final String serializedMessage = "{bareKey: 1, symbol: SYMBOL, timestamp: 2023-11-30T21:05:23.383Z, attribute: dollars::100.0 }";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(false));
        assertThat(parsedEvent.get("bareKey", Integer.class), equalTo(1));
        assertThat(parsedEvent.get("symbol", String.class), equalTo("SYMBOL"));
        assertThat(parsedEvent.get("timestamp", String.class), equalTo("2023-11-30T21:05:23.383Z"));
        assertThat(parsedEvent.get("attribute", Double.class), equalTo(100.0));

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }
}
