/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.ion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.plugins.processor.parse.AbstractParseProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

@DataPrepperPlugin(name = "parse_ion", pluginType = Processor.class, pluginConfigurationType = ParseIonProcessorConfig.class)
public class ParseIonProcessor extends AbstractParseProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ParseIonProcessor.class);
    private static final String PARSE_ERRORS = "parseErrors";

    private final IonObjectMapper objectMapper = new IonObjectMapper();

    private final Counter parseErrorsCounter;

    private final int depth;

    private final HandleFailedEventsOption handleFailedEventsOption;

    @DataPrepperPluginConstructor
    public ParseIonProcessor(final PluginMetrics pluginMetrics,
                             final ParseIonProcessorConfig parseIonProcessorConfig,
                             final ExpressionEvaluator expressionEvaluator,
                             final EventKeyFactory eventKeyFactory) {
        super(pluginMetrics, parseIonProcessorConfig, expressionEvaluator, eventKeyFactory);

        this.depth = parseIonProcessorConfig.getDepth();
        // Convert Timestamps to ISO-8601 Z strings
        objectMapper.registerModule(new IonTimestampConverterModule());

        handleFailedEventsOption = parseIonProcessorConfig.getHandleFailedEventsOption();
        parseErrorsCounter = pluginMetrics.counter(PARSE_ERRORS);
    }


    @Override
    protected Optional<Map<String, Object>> readValue(String message, Event context) {
        try {
            // We need to do a two-step process here, read the value in, then convert away any Ion types like Timestamp
            final HashMap<String, Object> map = objectMapper.convertValue(objectMapper.readValue(message, new TypeReference<>() {}), new TypeReference<>() {});
            if (depth == 0) {
                return Optional.of(map);
            }
            return Optional.of(objectMapper.convertValue(convertNestedObjectToString(map, 1, depth), new TypeReference<>() {}));
        } catch (JsonProcessingException e) {
            if (handleFailedEventsOption.shouldLog()) {
                LOG.error(SENSITIVE, "An exception occurred due to invalid Ion while parsing [{}] due to {}", message, e.getMessage());
            }
            parseErrorsCounter.increment();
            return Optional.empty();
        } catch (Exception e) {
            if (handleFailedEventsOption.shouldLog()) {
                LOG.error(SENSITIVE, "An exception occurred while using the parse_ion processor while parsing [{}]", message, e);
            }
            processingFailuresCounter.increment();
            return Optional.empty();
        }
    }
}
