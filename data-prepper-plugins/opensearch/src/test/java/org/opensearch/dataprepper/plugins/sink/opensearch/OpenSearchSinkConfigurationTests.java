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
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenSearchSinkConfigurationTests {
    private static final String OPEN_SEARCH_SINK_CONFIGURATIONS = "open-search-sink-configurations.yaml";
    private static final String VALID_SINK_CONFIG = "valid-sink";
    private static final String INVALID_ACTION_CONFIG = "invalid-action";
    private static final String INVALID_ACTIONS_CONFIG = "invalid-actions";
    private static final String INVALID_ACTION_WITH_EXPRESSION_CONFIG = "invalid-action-with-expression";
    private static final String INVALID_ACTIONS_WITH_EXPRESSION_CONFIG = "invalid-actions-with-expression";
    private static final String CREATE_ACTION_CONFIG = "create-action";
    private static final String CREATE_ACTIONS_WITH_EXPRESSION_CONFIG = "create-actions-with-expression";
    private ExpressionEvaluator expressionEvaluator;

    ObjectMapper objectMapper;

    @Test
    public void testReadESConfig() throws IOException {
        final OpenSearchSinkConfiguration openSearchSinkConfiguration = OpenSearchSinkConfiguration.readOSConfig(
                generateOpenSearchSinkConfig(VALID_SINK_CONFIG));
        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
        assertEquals(OpenSearchBulkActions.INDEX.toString(), openSearchSinkConfiguration.getIndexConfiguration().getAction());
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidAction() throws IOException {
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSinkConfig(INVALID_ACTION_CONFIG));
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidActions() throws IOException {
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSinkConfig(INVALID_ACTIONS_CONFIG));

    }

    @Test(expected = NullPointerException.class)
    public void testInvalidActionWithExpression() throws IOException {
        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidExpressionStatement(anyString())).thenReturn(false);
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSinkConfig(INVALID_ACTION_WITH_EXPRESSION_CONFIG), expressionEvaluator);
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidActionsWithExpression() throws IOException {
        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidExpressionStatement(anyString())).thenReturn(false);
        OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSinkConfig(INVALID_ACTIONS_WITH_EXPRESSION_CONFIG), expressionEvaluator);
    }

    @Test
    public void testReadOSConfigWithBulkActionCreate() throws IOException {
        final OpenSearchSinkConfiguration openSearchSinkConfiguration =
                OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSinkConfig(CREATE_ACTION_CONFIG));

        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
    }

    @Test
    public void testReadESConfigWithBulkActionCreateExpression() throws IOException {

        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.isValidFormatExpression("${getMetadata(\"action\")}")).thenReturn(true);

        final OpenSearchSinkConfiguration openSearchSinkConfiguration =
                OpenSearchSinkConfiguration.readOSConfig(generateOpenSearchSinkConfig(CREATE_ACTIONS_WITH_EXPRESSION_CONFIG));

        assertNotNull(openSearchSinkConfiguration.getConnectionConfiguration());
        assertNotNull(openSearchSinkConfiguration.getIndexConfiguration());
        assertNotNull(openSearchSinkConfiguration.getRetryConfiguration());
    }


    private OpenSearchSinkConfig generateOpenSearchSinkConfig(String pipelineName) throws IOException {
        final File configurationFile = new File(getClass().getClassLoader().getResource(OPEN_SEARCH_SINK_CONFIGURATIONS).getFile());
        objectMapper = new ObjectMapper(new YAMLFactory());
        final Map<String, Object> pipelineConfigs = objectMapper.readValue(configurationFile, Map.class);
        final Map<String, Object> pipelineConfig = (Map<String, Object>) pipelineConfigs.get(pipelineName);
        final Map<String, Object> sinkMap = (Map<String, Object>) pipelineConfig.get("sink");
        final Map<String, Object> opensearchSinkMap = (Map<String, Object>) sinkMap.get("opensearch");
        String json = objectMapper.writeValueAsString(opensearchSinkMap);
        OpenSearchSinkConfig openSearchSinkConfig = objectMapper.readValue(json, OpenSearchSinkConfig.class);

        return openSearchSinkConfig;
    }

}