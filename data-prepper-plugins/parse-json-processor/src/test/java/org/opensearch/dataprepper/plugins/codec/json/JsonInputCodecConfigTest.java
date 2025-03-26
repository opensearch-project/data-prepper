/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

public class JsonInputCodecConfigTest {

    private JsonInputCodecConfig createObjectUnderTest() {
        return new JsonInputCodecConfig();
    }

    @Test
    public void testJsonInputCodecConfig() {
        JsonInputCodecConfig jsonInputCodecConfig = createObjectUnderTest();
        assertNull(jsonInputCodecConfig.getKeyName());
        assertNull(jsonInputCodecConfig.getIncludeKeys());
        assertNull(jsonInputCodecConfig.getIncludeKeysMetadata());
        assertThat(jsonInputCodecConfig.getMaxEventLength(), equalTo(null));
    }
}
