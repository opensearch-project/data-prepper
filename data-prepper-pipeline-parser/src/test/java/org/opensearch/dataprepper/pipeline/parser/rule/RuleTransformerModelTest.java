/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleTransformerModelTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerialization() throws Exception {
        List<String> applyWhen = Arrays.asList("condition1", "condition2");
        List<String> functionProviders = Arrays.asList("org.example.Functions");
        String pluginName = "testPlugin";
        RuleTransformerModel model = new RuleTransformerModel(applyWhen, pluginName, functionProviders);

        String json = objectMapper.writeValueAsString(model);
        assertNotNull(json, "Serialized JSON should not be null");
        assertTrue(json.contains("\"function_providers\""));
        assertTrue(json.contains("org.example.Functions"));
    }

    @Test
    void testDeserialization() throws Exception {
        String json = "{\"plugin_name\": \"testPlugin\", \"apply_when\": [\"condition1\", \"condition2\"]}";

        RuleTransformerModel model = objectMapper.readValue(json, RuleTransformerModel.class);
        assertNotNull(model, "Deserialized model should not be null");
        assertEquals(2, model.getApplyWhen().size(), "ApplyWhen should contain two conditions");
        assertEquals("condition1", model.getApplyWhen().get(0), "The first condition should be 'condition1'");
        assertEquals("condition2", model.getApplyWhen().get(1), "The second condition should be 'condition2'");
        assertEquals("testPlugin", model.getPluginName(), "plugin Name");
        assertNull(model.getFunctionProviders());
    }

    @Test
    void testDeserialization_withFunctionProviders() throws Exception {
        String json = "{\"plugin_name\": \"rds\", \"apply_when\": [\"$..source.rds\"], " +
                "\"function_providers\": [\"org.opensearch.dataprepper.plugins.aws.PipelineTransformFunctions\"]}";

        RuleTransformerModel model = objectMapper.readValue(json, RuleTransformerModel.class);
        assertNotNull(model);
        assertEquals("rds", model.getPluginName());
        assertNotNull(model.getFunctionProviders());
        assertEquals(1, model.getFunctionProviders().size());
        assertEquals("org.opensearch.dataprepper.plugins.aws.PipelineTransformFunctions",
                model.getFunctionProviders().get(0));
    }

    @Test
    void testDeserialization_withMultipleFunctionProviders() throws Exception {
        String json = "{\"plugin_name\": \"test\", \"apply_when\": [\"$..source.test\"], " +
                "\"function_providers\": [\"com.example.Provider1\", \"com.example.Provider2\"]}";

        RuleTransformerModel model = objectMapper.readValue(json, RuleTransformerModel.class);
        assertNotNull(model.getFunctionProviders());
        assertEquals(2, model.getFunctionProviders().size());
        assertEquals("com.example.Provider1", model.getFunctionProviders().get(0));
        assertEquals("com.example.Provider2", model.getFunctionProviders().get(1));
    }

    @Test
    void testToString_includesFunctionProviders() {
        List<String> applyWhen = Arrays.asList("condition1");
        List<String> functionProviders = Arrays.asList("org.example.Funcs");
        RuleTransformerModel model = new RuleTransformerModel(applyWhen, "plugin", functionProviders);

        String result = model.toString();
        assertTrue(result.contains("functionProviders"));
        assertTrue(result.contains("org.example.Funcs"));
    }
}
