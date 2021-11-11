package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Converts Logstash plugin model to Data Prepper plugin model using mapping file
 *
 * @since 1.2
 */
public class DefaultPluginMapper implements LogstashPluginMapper {
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    private final AttributesMapperProvider attributesMapperProvider;

    public DefaultPluginMapper() {
        this(new AttributesMapperProvider());
    }

    DefaultPluginMapper(final AttributesMapperProvider attributesMapperProvider) {
        this.attributesMapperProvider = attributesMapperProvider;
    }

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

        final LogstashPluginAttributesMapper pluginAttributesMapper = attributesMapperProvider.getAttributesMapper(logstashMappingModel);

        final Map<String, Object> pluginSettings = pluginAttributesMapper.mapAttributes(logstashPlugin.getAttributes(), logstashMappingModel);

        return new PluginModel(logstashMappingModel.getPluginName(), pluginSettings);
    }
}
