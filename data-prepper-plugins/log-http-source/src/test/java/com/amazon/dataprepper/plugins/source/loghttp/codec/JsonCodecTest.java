/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp.codec;

import com.linecorp.armeria.common.HttpData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonCodecTest {
    private final HttpData goodTestData = HttpData.ofUtf8("[{\"a\":\"b\"}]");
    private final HttpData badTestData = HttpData.ofUtf8("{");
    private final JsonCodec objUnderTest = new JsonCodec();

    @Test
    public void testParseSuccess() throws IOException {
        // When
        List<String> res = objUnderTest.parse(goodTestData);

        // Then
        assertEquals(1, res.size());
        assertEquals("{\"a\":\"b\"}", res.get(0));
    }

    @Test
    public void testParseFailure() {
        assertThrows(IOException.class, () -> objUnderTest.parse(badTestData));
    }
}