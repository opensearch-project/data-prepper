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

        List<PluginModel> sourcePluginModels = mapPluginSection(logstashConfiguration, LogstashPluginType.INPUT);
        PluginModel sourcePlugin = null;
        if (sourcePluginModels.size() > 1)
            throw new LogstashMappingException("More than 1 source plugins are not supported");
        else if (sourcePluginModels.size() == 1)
            sourcePlugin = sourcePluginModels.get(0);

        List<PluginModel> prepperPluginModels = mapPluginSection(logstashConfiguration, LogstashPluginType.FILTER);

        List<PluginModel> sinkPluginModels = mapPluginSection(logstashConfiguration, LogstashPluginType.OUTPUT);

        return new PipelineModel(sourcePlugin, prepperPluginModels, sinkPluginModels, null, null);
    }

    private List<PluginModel> mapPluginSection(LogstashConfiguration logstashConfiguration, LogstashPluginType logstashPluginType) {
        LogstashPluginMapper pluginMapper = new LogstashPluginMapper();
        List<PluginModel> pluginModels = new LinkedList<>();

        List<LogstashPlugin> logstashPluginList = logstashConfiguration.getPluginSection(logstashPluginType);
        if (logstashPluginList != null)
            logstashPluginList.forEach(logstashPlugin -> pluginModels.add(pluginMapper.mapPlugin(logstashPlugin)));

        return pluginModels;
    }
}
