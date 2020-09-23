package com.amazon.situp.parser;

import com.amazon.situp.model.configuration.Configuration;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.parser.model.PipelineConfiguration;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Deserializer for pipeline configuration: Deserializes on the basis of {@link PipelineAttribute} for required check,
 * expects exactly one source, at least one sink, optional buffer and processors.
 */
public class ConfigurationDeserializer extends JsonDeserializer<PipelineConfiguration> {
    private static final ObjectMapper SIMPLE_OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    @Override
    public PipelineConfiguration deserialize(JsonParser jsonParser, DeserializationContext context) {
        PipelineConfiguration pipelineConfiguration;
        try {
            final JsonNode pipelineNode = jsonParser.getCodec().readTree(jsonParser);
            final JsonNode sourceNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.SOURCE);
            final JsonNode bufferNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.BUFFER);
            final JsonNode processorNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.PROCESSOR);
            final JsonNode sinkNode = getAttributeNodeFromPipeline(pipelineNode, PipelineAttribute.SINK);
            pipelineConfiguration = generatePipelineConfigModel(sourceNode, bufferNode, processorNode, sinkNode);
        } catch (IOException e) {
            throw new ParseException(e.getMessage(), e);
        }
        return pipelineConfiguration;
    }

    private PipelineConfiguration generatePipelineConfigModel(
            final JsonNode sourceNode,
            final JsonNode bufferNode,
            final JsonNode processorNode,
            final JsonNode sinkNode) {
        final Configuration sourceConfiguration = getConfigurationsOrEmpty(sourceNode);
        final Configuration bufferConfiguration = getConfigurationsOrEmpty(bufferNode);
        final Configuration processorConfiguration = getConfigurationsOrEmpty(processorNode);
        final Configuration sinkConfiguration = getConfigurationsOrEmpty(sinkNode);
        return new PipelineConfiguration(
                sourceConfiguration,
                bufferConfiguration,
                processorConfiguration,
                sinkConfiguration);
    }

    private Configuration getConfigurationsOrEmpty(final JsonNode jsonNode) {
        final List<PluginSetting> pluginSettings = new ArrayList<>();
        final Map<String, Object> attributeMap = new HashMap<>();
        if (jsonNode != null) {
            final Iterator<String> fieldIterator = jsonNode.fieldNames();
            while (fieldIterator.hasNext()) {
                String fieldName = fieldIterator.next();
                final JsonNode fieldValueNode = jsonNode.get(fieldName);
                if (fieldValueNode.isObject() || fieldValueNode.isNull()) { //to handle default source settings
                    final Map<String, Object> settingsMap = SIMPLE_OBJECT_MAPPER.convertValue(fieldValueNode, MAP_TYPE_REFERENCE);
                    pluginSettings.add(new PluginSetting(fieldName, settingsMap));
                } else if (fieldValueNode.isValueNode()) {
                    attributeMap.put(fieldName, fieldValueNode.asText());
                }
            }
        }
        return new Configuration(attributeMap, pluginSettings);
    }

    private JsonNode getAttributeNodeFromPipeline(final JsonNode pipelineNode, final PipelineAttribute pipelineAttribute) {
        return pipelineNode == null ? null : pipelineNode.get(pipelineAttribute.attributeName());
    }
}
