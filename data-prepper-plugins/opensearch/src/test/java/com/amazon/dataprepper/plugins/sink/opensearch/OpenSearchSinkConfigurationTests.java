/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class OpenSearchSinkConfigurationTests {
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "integTestPipeline";

    @Test
    public void testReadESConfig() {
        final OpenSearchSinkConfiguration openSearchSinkConfiguration = OpenSearchSinkConfiguration.readESConfig(
                generatePluginSetting());
        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
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
