/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Converts Logstash plugin model to Data Prepper plugin model using mapping file
 * {@link #LogstashPluginMapper(PluginMapperProvider)} is used for unit testing
 *
 * @since 1.2
 */
class LogstashPluginMapper {
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    private final PluginMapperProvider pluginMapperProvider;

    public LogstashPluginMapper() {
        this(new PluginMapperProvider());
    }

    public LogstashPluginMapper(final PluginMapperProvider pluginMapperProvider) {
        this.pluginMapperProvider = pluginMapperProvider;
    }

    public List<PluginModel> mapPlugin(LogstashPlugin logstashPlugin) {
        final String mappingResourceName = logstashPlugin.getPluginName() + ".mapping.yaml";

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

        if(logstashMappingModel.getCustomPluginMapperClass() != null) {
            final LogstashPluginAttributesMapper mapper = pluginMapperProvider.getAttributesMapper(logstashMappingModel);
            return mapper.mapAttributes(logstashPlugin.getAttributes(), logstashMappingModel);
        }

        if (logstashMappingModel.getPluginName() == null) {
            throw new LogstashMappingException("The mapping file " + mappingResourceName + " has a null value for 'pluginName'.");
        }

        final LogstashPluginAttributesMapper pluginAttributesMapper = pluginMapperProvider.getAttributesMapper(logstashMappingModel);

        final List<PluginModel> pluginModels = pluginAttributesMapper.mapAttributes(logstashPlugin.getAttributes(), logstashMappingModel);

        return pluginModels;
    }
}
