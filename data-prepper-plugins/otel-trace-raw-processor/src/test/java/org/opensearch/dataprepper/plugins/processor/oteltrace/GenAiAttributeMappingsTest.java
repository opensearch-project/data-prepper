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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
