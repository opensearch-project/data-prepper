/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleFileEvaluationTest {

    @Test
    void builder_createsObjectCorrectly() {
        List<String> functionProviders = Arrays.asList("com.example.Provider1", "com.example.Provider2");

        RuleFileEvaluation result = RuleFileEvaluation.builder()
                .withResult(true)
                .withRuleFileName("test-rule.yaml")
                .withPluginName("testPlugin")
                .withFunctionProviders(functionProviders)
                .build();

        assertTrue(result.getResult());
        assertEquals("test-rule.yaml", result.getRuleFileName());
        assertEquals("testPlugin", result.getPluginName());
        assertEquals(functionProviders, result.getFunctionProviders());
        assertEquals(2, result.getFunctionProviders().size());
    }

    @Test
    void builder_withNullFunctionProviders_setsNull() {
        RuleFileEvaluation result = RuleFileEvaluation.builder()
                .withResult(false)
                .withRuleFileName("rule.yaml")
                .withPluginName("plugin")
                .withFunctionProviders(null)
                .build();

        assertNull(result.getFunctionProviders());
    }

    @Test
    void defaultConstructor_initializesFieldsToNull() {
        RuleFileEvaluation result = new RuleFileEvaluation();

        assertNull(result.getResult());
        assertNull(result.getRuleFileName());
        assertNull(result.getPluginName());
        assertNull(result.getFunctionProviders());
    }

    @Test
    void allArgsConstructor_assignsFieldsCorrectly() {
        List<String> functionProviders = Arrays.asList("com.example.Provider");

        RuleFileEvaluation result = new RuleFileEvaluation(
                true, "rule.yaml", "myPlugin", functionProviders);

        assertTrue(result.getResult());
        assertEquals("rule.yaml", result.getRuleFileName());
        assertEquals("myPlugin", result.getPluginName());
        assertEquals(functionProviders, result.getFunctionProviders());
    }

    @Test
    void setter_updatesFunctionProviders() {
        RuleFileEvaluation result = new RuleFileEvaluation();
        List<String> providers = Arrays.asList("org.example.Functions");

        result.setFunctionProviders(providers);

        assertEquals(providers, result.getFunctionProviders());
    }
}
