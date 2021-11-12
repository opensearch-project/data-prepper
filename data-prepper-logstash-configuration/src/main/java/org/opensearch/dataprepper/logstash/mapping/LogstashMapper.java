package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PipelineModel;
import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;

import java.util.LinkedList;
import java.util.List;

/**
 * Converts Logstash configuration model to Data Prepper pipeline model
 *
 * @since 1.2
 */
public class LogstashMapper {
    public PipelineModel mapPipeline(LogstashConfiguration logstashConfiguration) {
        LogstashPluginMapper pluginMapper = new DefaultPluginMapper();

        List<PluginModel> sourcePluginModels = new LinkedList<>();
        List<PluginModel> prepperPluginModels = new LinkedList<>();
        List<PluginModel> sinkPluginModels = new LinkedList<>();

        List<LogstashPlugin> inputLogstashPluginList = logstashConfiguration.getPluginSection(LogstashPluginType.INPUT);
        if (inputLogstashPluginList != null)
            inputLogstashPluginList.forEach(logstashPlugin -> sourcePluginModels.add(pluginMapper.mapPlugin(logstashPlugin)));
        if (sourcePluginModels.size() > 1)
            throw new LogstashMappingException("More than 1 source plugins are not supported");

        List<LogstashPlugin> filterLogstashPluginList = logstashConfiguration.getPluginSection(LogstashPluginType.FILTER);
        if (filterLogstashPluginList != null)
            filterLogstashPluginList.forEach(logstashPlugin -> prepperPluginModels.add(pluginMapper.mapPlugin(logstashPlugin)));

        List<LogstashPlugin> outputLogstashPluginList = logstashConfiguration.getPluginSection(LogstashPluginType.OUTPUT);
        if (outputLogstashPluginList != null)
            outputLogstashPluginList.forEach(logstashPlugin -> sinkPluginModels.add(pluginMapper.mapPlugin(logstashPlugin)));

        return new PipelineModel(sourcePluginModels.get(0), prepperPluginModels, sinkPluginModels, 1, 3_000);
    }
}
