package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class MutateLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper{
    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {

    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return null;
    }
}
