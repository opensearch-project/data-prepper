package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;

public interface LogstashPluginMapper {

    /**
     * Takes a Logstash plugin with its associated attributes and maps it to corresponding
     * Data Prepper plugin and attributes based on Mapping files
     *
     * @param logstashPlugin A Logstash plugin with its attributes
     * @return A Data Prepper plugin with its attributes
     */
    PluginModel mapPlugin(LogstashPlugin logstashPlugin);
}
