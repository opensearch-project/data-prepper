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
    private final HttpData goodLargeTestData = HttpData.ofUtf8("[{\"a1\":\"b1\"}, {\"a2\":\"b2\"}, {\"a3\":\"b3\"}, {\"a4\":\"b4\"}, {\"a5\":\"b5\"}]");
    private final HttpData badTestDataJsonLine = HttpData.ofUtf8("{\"a\":\"b\"}");
    private final HttpData badTestDataMultiJsonLines = HttpData.ofUtf8("{\"a\":\"b\"}{\"c\":\"d\"}");
    private final HttpData badTestDataNonJson = HttpData.ofUtf8("non json content");
    private final JsonCodec objectUnderTest = new JsonCodec();

    @Test
    public void testParseSuccess() throws IOException {
        // When
        List<List<String>> res = objectUnderTest.parse(goodTestData);

        // Then
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
        assertEquals("{\"a\":\"b\"}", res.get(0).get(0));
    }

    @Test
    public void testParseSuccessWithMaxSize() throws IOException {
        // When
        List<List<String>> res = objectUnderTest.parse(goodLargeTestData, 30);

        assertEquals(3, res.size());

        // Then
        assertEquals(2, res.get(0).size());
        assertEquals("{\"a1\":\"b1\"}", res.get(0).get(0));
        assertEquals("{\"a2\":\"b2\"}", res.get(0).get(1));
        assertEquals(2, res.get(1).size());
        assertEquals("{\"a3\":\"b3\"}", res.get(1).get(0));
        assertEquals("{\"a4\":\"b4\"}", res.get(1).get(1));
        assertEquals(1, res.get(2).size());
        assertEquals("{\"a5\":\"b5\"}", res.get(2).get(0));
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