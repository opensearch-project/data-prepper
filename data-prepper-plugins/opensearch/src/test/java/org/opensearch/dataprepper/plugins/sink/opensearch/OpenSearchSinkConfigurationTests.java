/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.Test;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenSearchSinkConfigurationTests {
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "integTestPipeline";
    private ExpressionEvaluator expressionEvaluator;

    @Test
    public void testReadESConfig() {
        final OpenSearchSinkConfiguration openSearchSinkConfiguration = OpenSearchSinkConfiguration.readESConfig(
                generatePluginSetting());
        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
        assertEquals(OpenSearchBulkActions.INDEX.toString(), openSearchSinkConfiguration.getIndexConfiguration().getAction());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAction() {

        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.TRACE_ANALYTICS_RAW.getValue());
        metadata.put(IndexConfiguration.ACTION, "invalid");
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);

        OpenSearchSinkConfiguration.readESConfig(pluginSetting);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidActions() {

        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.TRACE_ANALYTICS_RAW.getValue());
        List<Map<String, Object>> invalidActionList = new ArrayList<>();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put("type", "invalid");
        invalidActionList.add(actionMap);
        metadata.put(IndexConfiguration.ACTIONS, invalidActionList);
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);

        OpenSearchSinkConfiguration.readESConfig(pluginSetting);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidActionWithExpression() {

        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.TRACE_ANALYTICS_RAW.getValue());
        metadata.put(IndexConfiguration.ACTION, "${anInvalidFunction()}");
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);

        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidExpressionStatement(anyString())).thenReturn(false);
        OpenSearchSinkConfiguration.readESConfig(pluginSetting, expressionEvaluator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidActionsWithExpression() {

        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.TRACE_ANALYTICS_RAW.getValue());
        List<Map<String, Object>> invalidActionList = new ArrayList<>();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put("type", "${anInvalidFunction()}");
        invalidActionList.add(actionMap);
        metadata.put(IndexConfiguration.ACTIONS, invalidActionList);
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);

        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidExpressionStatement(anyString())).thenReturn(false);
        OpenSearchSinkConfiguration.readESConfig(pluginSetting, expressionEvaluator);
    }

    @Test
    public void testReadESConfigWithBulkActionCreate() {

        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.TRACE_ANALYTICS_RAW.getValue());
        metadata.put(IndexConfiguration.ACTION, OpenSearchBulkActions.CREATE.toString());
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);

        final OpenSearchSinkConfiguration openSearchSinkConfiguration =
                OpenSearchSinkConfiguration.readESConfig(pluginSetting);

        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
    }

    @Test
    public void testReadESConfigWithBulkActionCreateExpression() {

        final String actionFormatExpression = "${getMetadata(\"action\")}";
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.TRACE_ANALYTICS_RAW.getValue());
        metadata.put(IndexConfiguration.ACTION, actionFormatExpression);
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);

        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidFormatExpressions(actionFormatExpression)).thenReturn(true);
        final OpenSearchSinkConfiguration openSearchSinkConfiguration =
                OpenSearchSinkConfiguration.readESConfig(pluginSetting, expressionEvaluator);

        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
    }

    private PluginSetting generatePluginSetting() {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.TRACE_ANALYTICS_RAW.getValue());
        metadata.put(ConnectionConfiguration.HOSTS, TEST_HOSTS);

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, metadata);
        pluginSetting.setPipelineName(PIPELINE_NAME);
        return pluginSetting;
    }
}
