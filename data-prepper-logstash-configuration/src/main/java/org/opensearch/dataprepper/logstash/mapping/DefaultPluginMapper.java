package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts Logstash plugin model to Data Prepper plugin model using mapping file
 *
 * @since 1.2
 */
public class DefaultPluginMapper implements LogstashPluginMapper {
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    @Override
    public PluginModel mapPlugin(LogstashPlugin logstashPlugin) throws IOException {

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(logstashPlugin.getPluginName() + ".mapping.yaml");
        if (inputStream == null) {
            throw new LogstashMappingException("file not found! " + logstashPlugin.getPluginName() + ".mapping.yaml");
        }

        LogstashMappingModel logstashMappingModel = objectMapper.readValue(inputStream, LogstashMappingModel.class);
        if (logstashMappingModel.getPluginName() == null)
            throw new LogstashMappingException("plugin name cannot be null in mapping.yaml");

        Map<String, Object> pluginSettings = new LinkedHashMap<>(logstashMappingModel.getAdditionalAttributes());

        logstashPlugin.getAttributes().forEach(logstashAttribute -> {
            if (logstashMappingModel.getMappedAttributes().containsKey(logstashAttribute.getAttributeName())) {
                pluginSettings.put(
                        (String) logstashMappingModel.getMappedAttributes().get(logstashAttribute.getAttributeName()),
                        logstashAttribute.getAttributeValue().getValue()
                );
            }
        });

        return new PluginModel(logstashMappingModel.getPluginName(), pluginSettings);
    }
}
