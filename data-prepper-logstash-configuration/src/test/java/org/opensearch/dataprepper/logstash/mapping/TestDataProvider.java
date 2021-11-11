package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestDataProvider {

    public static LogstashPlugin invalidMappingResourceNameData() {
        return LogstashPlugin.builder()
                .pluginName("amazon_elasticsearch")
                .attributes(Collections.singletonList(getArrayTypeAttribute())).build();
    }

    public static LogstashPlugin invalidMappingResourceData() {
        return LogstashPlugin.builder()
                .pluginName("invalid")
                .attributes(Collections.singletonList(getArrayTypeAttribute())).build();
    }

    public static LogstashPlugin noPluginNameMappingResourceData() {
        return LogstashPlugin.builder()
                .pluginName("no_plugin_name")
                .attributes(Collections.singletonList(getArrayTypeAttribute())).build();
    }

    public static LogstashPlugin pluginData() {
        return LogstashPlugin.builder()
                .pluginName("amazon_es")
                .attributes(Arrays.asList(getArrayTypeAttribute(), getStringTypeAttribute())).build();
    }

    public static PluginModel getSamplePluginModel() {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("aws_sigv4", true);
        attributes.put("insecure", false);
        attributes.put("aws_region", "us-west-2");
        attributes.put("hosts", Collections.singletonList("https://localhost:9200"));
        return new PluginModel("opensearch", attributes);
    }

    private static LogstashAttribute getArrayTypeAttribute() {
        LogstashAttributeValue logstashAttributeValue = LogstashAttributeValue.builder()
                .attributeValueType(LogstashValueType.ARRAY)
                .value(Collections.singletonList("https://localhost:9200"))
                .build();
        return LogstashAttribute.builder()
                .attributeName("hosts")
                .attributeValue(logstashAttributeValue)
                .build();
    }

    private static LogstashAttribute getStringTypeAttribute() {
        LogstashAttributeValue logstashAttributeValue = LogstashAttributeValue.builder()
                .attributeValueType(LogstashValueType.STRING)
                .value("us-west-2")
                .build();
        return LogstashAttribute.builder()
                .attributeName("region")
                .attributeValue(logstashAttributeValue)
                .build();
    }

}
