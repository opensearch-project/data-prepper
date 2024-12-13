/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenSearchSinkConfigurationTests {
    private static final String VALID_SINK_CONFIG = "test-configurations/valid_sink_config.yaml";
    private static final String INVALID_ACTION_CONFIG = "test-configurations/invalid-action-config.yaml";
    private static final String INVALID_ACTIONS_CONFIG = "test-configurations/invalid-actions-config.yaml";
    private static final String INVALID_ACTION_WITH_EXPRESSION_CONFIG = "test-configurations/invalid-action-with-expression-config.yaml";
    private static final String INVALID_ACTIONS_WITH_EXPRESSION_CONFIG = "test-configurations/invalid-actions-with-expression-config.yaml";
    private static final String CREATE_ACTION_CONFIG = "test-configurations/create-action-config.yaml";
    private static final String CREATE_ACTIONS_WITH_EXPRESSION_CONFIG = "test-configurations/create-actions-with-expression-config.yaml";
    private ExpressionEvaluator expressionEvaluator;

    ObjectMapper objectMapper;

    @Test
    public void testReadESConfig() throws IOException {
        final OpenSearchSinkConfiguration openSearchSinkConfiguration = OpenSearchSinkConfiguration.readOSConfig(
                generateOpenSearchSourceConfig(VALID_SINK_CONFIG));
        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
        assertEquals(OpenSearchBulkActions.INDEX.toString(), openSearchSinkConfiguration.getIndexConfiguration().getAction());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAction() throws IOException {
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSourceConfig(INVALID_ACTION_CONFIG));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidActions() throws IOException {
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSourceConfig(INVALID_ACTIONS_CONFIG));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidActionWithExpression() throws IOException {
        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidExpressionStatement(anyString())).thenReturn(false);
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSourceConfig(INVALID_ACTION_WITH_EXPRESSION_CONFIG), expressionEvaluator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidActionsWithExpression() throws IOException {
        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidExpressionStatement(anyString())).thenReturn(false);
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSourceConfig(INVALID_ACTIONS_WITH_EXPRESSION_CONFIG), expressionEvaluator);
    }

    @Test
    public void testReadOSConfigWithBulkActionCreate() throws IOException {
        final OpenSearchSinkConfiguration openSearchSinkConfiguration =
                OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSourceConfig(CREATE_ACTION_CONFIG));

        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
    }

    @Test
    public void testReadESConfigWithBulkActionCreateExpression() throws IOException {

        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidFormatExpression("${getMetadata(\"action\")}")).thenReturn(true);

        final OpenSearchSinkConfiguration openSearchSinkConfiguration =
                OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSourceConfig(CREATE_ACTIONS_WITH_EXPRESSION_CONFIG));

        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
    }

    private OpenSearchSinkConfig generateOpenSearchSourceConfig(String yamlFile) throws IOException {
        final File configurationFile = new File(getClass().getClassLoader().getResource(yamlFile).getFile());
        objectMapper = new ObjectMapper(new YAMLFactory());
        final Map<String, Object> pipelineConfig = objectMapper.readValue(configurationFile, Map.class);
        final Map<String, Object> sinkMap = (Map<String, Object>) pipelineConfig.get("sink");
        final Map<String, Object> opensearchSinkMap = (Map<String, Object>) sinkMap.get("opensearch");
        String json = objectMapper.writeValueAsString(opensearchSinkMap);
        OpenSearchSinkConfig openSearchSinkConfig = objectMapper.readValue(json, OpenSearchSinkConfig.class);

        return openSearchSinkConfig;
    }
}