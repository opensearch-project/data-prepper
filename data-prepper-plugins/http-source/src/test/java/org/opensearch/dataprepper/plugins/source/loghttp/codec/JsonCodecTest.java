/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp.codec;

import com.linecorp.armeria.common.HttpData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonCodecTest {
    private final HttpData goodTestData = HttpData.ofUtf8("[{\"a\":\"b\"}, {\"c\":\"d\"}]");
    private final HttpData badTestDataJsonLine = HttpData.ofUtf8("{\"a\":\"b\"}");
    private final HttpData badTestDataMultiJsonLines = HttpData.ofUtf8("{\"a\":\"b\"}{\"c\":\"d\"}");
    private final HttpData badTestDataNonJson = HttpData.ofUtf8("non json content");
    private final JsonCodec objectUnderTest = new JsonCodec();

    @Test
    public void testParseSuccess() throws IOException {
        // When
        List<String> res = objectUnderTest.parse(goodTestData);

        // Then
        assertEquals(2, res.size());
        assertEquals("{\"a\":\"b\"}", res.get(0));
    }

    @Test
    public void testParseJsonLineFailure() {
        assertThrows(IOException.class, () -> objectUnderTest.parse(badTestDataJsonLine));
    }

    @Test
    public void testParseMultiJsonLinesFailure() {
        assertThrows(IOException.class, () -> objectUnderTest.parse(badTestDataMultiJsonLines));
    }

    @Test
    public void testParseNonJsonFailure() {
        assertThrows(IOException.class, () -> objectUnderTest.parse(badTestDataNonJson));
    }
}