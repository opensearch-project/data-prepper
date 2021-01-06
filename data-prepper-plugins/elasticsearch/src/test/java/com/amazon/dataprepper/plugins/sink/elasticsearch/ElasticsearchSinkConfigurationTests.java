package com.amazon.dataprepper.plugins.sink.elasticsearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class ElasticsearchSinkConfigurationTests {
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private static final String PLUGIN_NAME = "elasticsearch";
    private static final String PIPELINE_NAME = "integTestPipeline";

    @Test
    public void testReadESConfig() {
        final ElasticsearchSinkConfiguration elasticsearchSinkConfiguration = ElasticsearchSinkConfiguration.readESConfig(
                generatePluginSetting());
        assertNotNull(elasticsearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(elasticsearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(elasticsearchSinkConfiguration.getRetryConfiguration());
    }

    private PluginSetting generatePluginSetting() {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.TRACE_ANALYTICS_RAW_FLAG, true);
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);
        return pluginSetting;
    }
}
