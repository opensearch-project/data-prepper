/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.ion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.processor.parse.AbstractParseProcessor;
import org.opensearch.dataprepper.plugins.processor.parse.json.ParseJsonProcessorTest;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
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
    }

    @Override
    protected AbstractParseProcessor createObjectUnderTest() {
        return new ParseIonProcessor(pluginMetrics, ionProcessorConfig, expressionEvaluator);
    }

    @Test
    void test_when_using_ion_features_then_processorParsesCorrectly() {
        parseJsonProcessor = createObjectUnderTest();

        final String serializedMessage = "{bareKey: 1, symbol: SYMBOL, timestamp: 2023-11-30T21:05:23.383Z, attribute: dollars::100.0 }";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.get("bareKey", Integer.class), equalTo(1));
        assertThat(parsedEvent.get("symbol", String.class), equalTo("SYMBOL"));
        assertThat(parsedEvent.get("timestamp", String.class), equalTo("2023-11-30T21:05:23.383Z"));
        assertThat(parsedEvent.get("attribute", Double.class), equalTo(100.0));
    }
}
