/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;

class RuleTransformerModelTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerialization() throws Exception {
        List<String> applyWhen = Arrays.asList("condition1", "condition2");
        String pluginName = "testPlugin";
        RuleTransformerModel model = new RuleTransformerModel(applyWhen, pluginName);

        String json = objectMapper.writeValueAsString(model);
        assertNotNull(json, "Serialized JSON should not be null");
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
    }
}
