package org.opensearch.dataprepper.plugins.processor.parse.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
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
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

@DataPrepperPlugin(name = "parse_xml", pluginType =Processor.class, pluginConfigurationType =ParseXmlProcessorConfig.class)
public class ParseXmlProcessor extends AbstractParseProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ParseXmlProcessor.class);
    private static final String PARSE_ERRORS = "parseErrors";

    private final XmlMapper xmlMapper = new XmlMapper();
    private final Counter parseErrorsCounter;

    private final HandleFailedEventsOption handleFailedEventsOption;

    @DataPrepperPluginConstructor
    public ParseXmlProcessor(final PluginMetrics pluginMetrics,
                              final ParseXmlProcessorConfig parseXmlProcessorConfig,
                              final ExpressionEvaluator expressionEvaluator,
                              final EventKeyFactory eventKeyFactory) {
        super(pluginMetrics, parseXmlProcessorConfig, expressionEvaluator, eventKeyFactory);

        handleFailedEventsOption = parseXmlProcessorConfig.getHandleFailedEventsOption();
        parseErrorsCounter = pluginMetrics.counter(PARSE_ERRORS);
    }

    @Override
    protected Optional<HashMap<String, Object>> readValue(final String message, final Event context) {
        try {
            return Optional.of(xmlMapper.readValue(message, new TypeReference<>() {}));
        } catch (JsonProcessingException e) {
            if (handleFailedEventsOption.shouldLog()) {
                LOG.error(SENSITIVE, "An exception occurred due to invalid XML while parsing [{}] due to {}", message, e.getMessage());
            }
            parseErrorsCounter.increment();
            return Optional.empty();
        } catch (Exception e) {
            if (handleFailedEventsOption.shouldLog()) {
                LOG.error(SENSITIVE, "An exception occurred while using the parse_xml processor while parsing [{}]", message, e);
            }
            processingFailuresCounter.increment();
            return Optional.empty();
        }
    }
}
