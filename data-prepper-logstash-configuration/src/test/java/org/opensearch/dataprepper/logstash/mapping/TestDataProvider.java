/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
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
import java.util.UUID;


public class TestDataProvider {

    public static PluginModel samplePluginModel() {
        Map<String, Object> pluginSettings = new LinkedHashMap<>();
        pluginSettings.put("hosts", Collections.singletonList("https://localhost:9200"));
        pluginSettings.put("aws_region", "us-west-2");
        pluginSettings.put("aws_sigv4", true);
        pluginSettings.put("insecure", false);

        return new PluginModel("opensearch", pluginSettings);
    }

    static SinkModel sampleSinkModel() {
        final PluginModel pluginModel = samplePluginModel();

        return SinkModel.builder(pluginModel).build();
    }

    public static LogstashConfiguration sampleConfigurationWithMoreThanOnePlugin() {
        List<LogstashPlugin> logstashPluginList = Arrays.asList(pluginData(), pluginData());
        return LogstashConfiguration.builder()
                .pluginSections(Collections.singletonMap(LogstashPluginType.INPUT, logstashPluginList))
                .build();
    }

    public static LogstashConfiguration sampleConfigurationWithEmptyInputPlugins() {
        return LogstashConfiguration.builder()
                .pluginSections(Collections.singletonMap(LogstashPluginType.INPUT, Collections.emptyList()))
                .build();
    }

    public static LogstashPlugin invalidMappingResourceNameData() {
        return LogstashPlugin.builder()
                .pluginName(UUID.randomUUID().toString())
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

    public static LogstashPlugin mutatePlugin() {
        return LogstashPlugin.builder()
                .pluginName("mutate")
                .attributes(Collections.singletonList(getArrayTypeAttribute())).build();
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
