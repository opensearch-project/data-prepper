/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@DataPrepperPlugin(name = "parse_json", pluginType = Processor.class, pluginConfigurationType = ParseJsonProcessorConfig.class)
public class ParseJsonProcessor extends AbstractParseProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ParseJsonProcessor.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @DataPrepperPluginConstructor
    public ParseJsonProcessor(final PluginMetrics pluginMetrics,
                              final ParseJsonProcessorConfig parseJsonProcessorConfig,
                              final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics, parseJsonProcessorConfig, expressionEvaluator);
    }

    @Override
    protected Optional<HashMap<String, Object>> readValue(String message, Event context) {
        try {
            return Optional.of(objectMapper.readValue(message, new TypeReference<>() {}));
        } catch (JsonProcessingException e) {
            LOG.error(SENSITIVE, "An exception occurred due to invalid JSON while parsing [{}] due to {}", message, e.getMessage());
            return Optional.empty();
        } catch (Exception e) {
            LOG.error(SENSITIVE, "An exception occurred while using the parse_json processor while parsing [{}]", message, e);
            return Optional.empty();
        }
    }
}
