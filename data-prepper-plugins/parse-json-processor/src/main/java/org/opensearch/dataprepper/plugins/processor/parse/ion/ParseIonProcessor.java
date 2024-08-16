/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.ion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.plugins.processor.parse.AbstractParseProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

@DataPrepperPlugin(name = "parse_ion", pluginType = Processor.class, pluginConfigurationType = ParseIonProcessorConfig.class)
public class ParseIonProcessor extends AbstractParseProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ParseIonProcessor.class);

    private final IonObjectMapper objectMapper = new IonObjectMapper();

    @DataPrepperPluginConstructor
    public ParseIonProcessor(final PluginMetrics pluginMetrics,
                             final ParseIonProcessorConfig parseIonProcessorConfig,
                             final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics, parseIonProcessorConfig, expressionEvaluator);

        // Convert Timestamps to ISO-8601 Z strings
        objectMapper.registerModule(new IonTimestampConverterModule());
    }

    @Override
    protected Optional<HashMap<String, Object>> readValue(String message, Event context) {
        try {
            // We need to do a two-step process here, read the value in, then convert away any Ion types like Timestamp
            return Optional.of(objectMapper.convertValue(objectMapper.readValue(message, new TypeReference<>() {}), new TypeReference<>() {}));
        } catch (JsonProcessingException e) {
            LOG.error(SENSITIVE, "An exception occurred due to invalid Ion while parsing [{}] due to {}", message, e.getMessage());
            return Optional.empty();
        } catch (Exception e) {
            LOG.error(SENSITIVE, "An exception occurred while using the parse_ion processor while parsing [{}]", message, e);
            return Optional.empty();
        }
    }
}
