package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(DefaultPluginMapper.class);
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    @Override
    public PluginModel mapPlugin(LogstashPlugin logstashPlugin) {

        String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

        final InputStream inputStream = this.getClass().getResourceAsStream(mappingResourceName);
        if (inputStream == null) {
            throw new LogstashMappingException("Unable to find mapping resource " + mappingResourceName);
        }

        LogstashMappingModel logstashMappingModel;
        try {
            logstashMappingModel = objectMapper.readValue(inputStream, LogstashMappingModel.class);
        }
        catch(IOException ex) {
            throw new LogstashMappingException("Unable to parse mapping file " + mappingResourceName, ex);
        }

        if (logstashMappingModel.getPluginName() == null) {
            throw new LogstashMappingException("The mapping file " + mappingResourceName + " has a null value for 'pluginName'.");
        }
        Map<String, Object> pluginSettings = new LinkedHashMap<>(logstashMappingModel.getAdditionalAttributes());

        logstashPlugin.getAttributes().forEach(logstashAttribute -> {
            if (logstashMappingModel.getMappedAttributeNames().containsKey(logstashAttribute.getAttributeName())) {
                pluginSettings.put(
                        logstashMappingModel.getMappedAttributeNames().get(logstashAttribute.getAttributeName()),
                        logstashAttribute.getAttributeValue().getValue()
                );
            }
            else {
                LOG.warn("Attribute name {} is not found in mapping file.", logstashAttribute.getAttributeName());
            }
        });

        return new PluginModel(logstashMappingModel.getPluginName(), pluginSettings);
    }
}
