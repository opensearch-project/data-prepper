/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonInputCodecConfigTest {

    private JsonInputCodecConfig createObjectUnderTest() {
        return new JsonInputCodecConfig();
    }

    @Test
    public void testJsonInputCodecConfig() {
        JsonInputCodecConfig jsonInputCodecConfig = createObjectUnderTest();
        assertTrue(jsonInputCodecConfig.getKeyName().equals(JsonInputCodecConfig.DEFAULT_KEY_NAME));
        assertNull(jsonInputCodecConfig.getIncludeKeys());
        assertNull(jsonInputCodecConfig.getIncludeKeysMetadata());
    }
}
