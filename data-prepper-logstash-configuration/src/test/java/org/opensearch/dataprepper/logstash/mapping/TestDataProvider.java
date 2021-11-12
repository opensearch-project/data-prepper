package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;


public class TestDataProvider {

    public static LogstashConfiguration sampleConfiguration() {
        return LogstashConfiguration.builder()
                .pluginSections(Collections.singletonMap(LogstashPluginType.INPUT, Collections.singletonList(pluginData())))
                .build();
    }

    public static PluginModel samplePluginModel() {
        Map<String, Object> pluginSettings = new LinkedHashMap<>();
        pluginSettings.put("hosts", Collections.singletonList("https://localhost:9200"));
        pluginSettings.put("aws_region", "us-west-2");
        pluginSettings.put("aws_sigv4", true);
        pluginSettings.put("insecure", false);

        return new PluginModel("opensearch", pluginSettings);
    }

    public static LogstashConfiguration sampleConfigurationWithMoreThanOnePlugin() {
        List<LogstashPlugin> logstashPluginList = Arrays.asList(pluginData(), pluginData());
        return LogstashConfiguration.builder()
                .pluginSections(Collections.singletonMap(LogstashPluginType.INPUT, logstashPluginList))
                .build();
    }

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

//    public static PluginModel getSamplePluginModel() {
//        Map<String, Object> attributes = new LinkedHashMap<>();
//        attributes.put("aws_sigv4", true);
//        attributes.put("insecure", false);
//        attributes.put("aws_region", "us-west-2");
//        attributes.put("hosts", Collections.singletonList("https://localhost:9200"));
//        return new PluginModel("opensearch", attributes);
//    }

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
