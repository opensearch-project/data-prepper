package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

import java.util.List;

public interface CustomPluginMapper {
    List<PluginModel> mapPlugin(LogstashPlugin logstashPlugin);
}
