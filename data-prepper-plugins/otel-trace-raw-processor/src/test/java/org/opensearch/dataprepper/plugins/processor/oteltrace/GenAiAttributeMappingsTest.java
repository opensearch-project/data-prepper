/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GenAiAttributeMappingsTest {

    @Test
    void testMappingsFileExists() {
        final InputStream is = GenAiAttributeMappings.class.getClassLoader()
                .getResourceAsStream(GenAiAttributeMappings.MAPPINGS_FILE);
        assertNotNull(is, "Mappings file should exist in resources: " + GenAiAttributeMappings.MAPPINGS_FILE);
    }

    @Test
    void testLookupTableIsNonEmpty() {
        assertFalse(GenAiAttributeMappings.getLookupTable().isEmpty());
    }

    @Test
    void testOperationNameValuesIsNonEmpty() {
        assertFalse(GenAiAttributeMappings.getOperationNameValues().isEmpty());
    }

    @Test
    void testOpenInferenceMappingPresent() {
        final GenAiAttributeMappings.MappingTarget target =
                GenAiAttributeMappings.getLookupTable().get("llm.token_count.prompt");
        assertNotNull(target);
        assertEquals("gen_ai.usage.input_tokens", target.getKey());
    }

    @Test
    void testOpenLLMetryMappingPresent() {
        final GenAiAttributeMappings.MappingTarget target =
                GenAiAttributeMappings.getLookupTable().get("llm.usage.prompt_tokens");
        assertNotNull(target);
        assertEquals("gen_ai.usage.input_tokens", target.getKey());
    }

    @Test
    void testWrapSliceMappingPresent() {
        final GenAiAttributeMappings.MappingTarget target =
                GenAiAttributeMappings.getLookupTable().get("llm.response.finish_reason");
        assertNotNull(target);
        assertEquals("gen_ai.response.finish_reasons", target.getKey());
        assertEquals(true, target.isWrapAsArray());
    }

    @Test
    void testOperationNameValueMapping() {
        assertEquals("chat", GenAiAttributeMappings.getOperationNameValues().get("llm"));
        assertEquals("invoke_agent", GenAiAttributeMappings.getOperationNameValues().get("agent"));
        assertEquals("embeddings", GenAiAttributeMappings.getOperationNameValues().get("embedding"));
    }

    @Test
    void testPriorityFollowsFileOrderAcrossProfiles() {
        final Map<String, Object> yaml = new LinkedHashMap<>();
        yaml.put("first_profile", List.of(
                Map.of("from", "a.session", "to", "gen_ai.conversation.id"),
                Map.of("from", "a.tokens", "to", "gen_ai.usage.input_tokens")));
        yaml.put("second_profile", List.of(
                Map.of("from", "b.session", "to", "gen_ai.conversation.id")));

        final Map<String, GenAiAttributeMappings.MappingTarget> table =
                GenAiAttributeMappings.buildLookupTable(yaml);

        assertEquals(0, table.get("a.session").getPriority());
        assertEquals(1, table.get("a.tokens").getPriority());
        assertEquals(2, table.get("b.session").getPriority());
    }

    @Test
    void testDuplicateSourceKeepsFirstTargetAndPriority() {
        final Map<String, Object> yaml = new LinkedHashMap<>();
        yaml.put("first_profile", List.of(Map.of("from", "shared.key", "to", "gen_ai.agent.name")));
        yaml.put("second_profile", List.of(
                Map.of("from", "shared.key", "to", "gen_ai.conversation.id"),
                Map.of("from", "other.key", "to", "gen_ai.conversation.id")));

        final Map<String, GenAiAttributeMappings.MappingTarget> table =
                GenAiAttributeMappings.buildLookupTable(yaml);

        assertEquals("gen_ai.agent.name", table.get("shared.key").getKey());
        assertEquals(0, table.get("shared.key").getPriority());
        assertTrue(table.get("other.key").getPriority() > table.get("shared.key").getPriority());
    }

    @Test
    void testEntriesWithoutFromOrToAreSkippedAndDoNotTakeAPriority() {
        final Map<String, Object> yaml = new LinkedHashMap<>();
        yaml.put("profile", List.of(
                Map.of("to", "gen_ai.agent.name"),
                Map.of("from", "no.target"),
                "not a mapping",
                Map.of("from", "valid.key", "to", "gen_ai.agent.name")));
        yaml.put("operation_name_values", Map.of("llm", "chat"));

        final Map<String, GenAiAttributeMappings.MappingTarget> table =
                GenAiAttributeMappings.buildLookupTable(yaml);

        assertEquals(Set.of("valid.key"), table.keySet());
        assertEquals(0, table.get("valid.key").getPriority());
    }

    @Test
    void testOpenInferenceSessionIdOutranksTraceloopSessionId() {
        final Map<String, GenAiAttributeMappings.MappingTarget> table = GenAiAttributeMappings.getLookupTable();
        final GenAiAttributeMappings.MappingTarget openInference = table.get("session.id");
        final GenAiAttributeMappings.MappingTarget traceloop =
                table.get("traceloop.association.properties.session_id");

        assertEquals("gen_ai.conversation.id", openInference.getKey());
        assertEquals("gen_ai.conversation.id", traceloop.getKey());
        assertTrue(openInference.getPriority() < traceloop.getPriority());
    }

    @Test
    void testShippedMappingsHaveDistinctPriorities() {
        final Set<Integer> priorities = new HashSet<>();
        GenAiAttributeMappings.getLookupTable().values()
                .forEach(target -> assertTrue(priorities.add(target.getPriority()),
                        "Duplicate priority " + target.getPriority()));
    }
}
