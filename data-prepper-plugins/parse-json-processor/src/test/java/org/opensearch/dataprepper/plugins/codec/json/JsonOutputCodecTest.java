/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


public class JsonOutputCodecTest {

    private JsonOutputCodec createObjectUnderTest() {
        return new JsonOutputCodec();
    }

    @Test
    void testGetExtension() {
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();
        assertThat(null, equalTo(jsonOutputCodec.getExtension()));
    }
}