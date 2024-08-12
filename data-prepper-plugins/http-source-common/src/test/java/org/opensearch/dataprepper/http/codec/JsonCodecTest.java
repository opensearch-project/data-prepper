/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.codec;

import com.linecorp.armeria.common.HttpData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class JsonCodecTest {
    private final HttpData goodTestData = HttpData.ofUtf8("[{\"a\":\"b\"}, {\"c\":\"d\"}]");
    private final HttpData goodLargeTestData = HttpData.ofUtf8("[{\"a1\":\"b1\"}, {\"a2\":\"b2\"}, {\"a3\":\"b3\"}, {\"a4\":\"b4\"}, {\"a5\":\"b5\"}]");
    private final HttpData goodLargeTestDataUnicode = HttpData.ofUtf8("[{\"ὊὊὊ1\":\"ὊὊὊ1\"}, {\"ὊὊὊ2\":\"ὊὊὊ2\"}, {\"a3\":\"b3\"}, {\"ὊὊὊ4\":\"ὊὊὊ4\"}]");
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

    @ParameterizedTest
    @ArgumentsSource(JsonArrayWithKnownFirstArgumentsProvider.class)
    public void parse_should_return_lists_smaller_than_provided_length(
            final String inputJsonArray, final String knownFirstPart, int expectedChunks) throws IOException {
        final int knownSingleBodySize = knownFirstPart.getBytes(Charset.defaultCharset()).length;
        final int maxSize = (knownSingleBodySize * 2) + 3;
        final List<List<String>> chunkedBodies = objectUnderTest.parse(HttpData.ofUtf8(inputJsonArray),
                maxSize);

        assertThat(chunkedBodies, notNullValue());
        assertThat(chunkedBodies.size(), equalTo(expectedChunks));
        final String firstReconstructed = chunkedBodies.get(0).stream().collect(Collectors.joining(",", "[", "]"));
        assertThat(firstReconstructed.getBytes(Charset.defaultCharset()).length,
                lessThanOrEqualTo(maxSize));

        assertThat(chunkedBodies.get(0).size(), greaterThanOrEqualTo(1));
        assertThat(chunkedBodies.get(0).get(0), equalTo(knownFirstPart));
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

    static class JsonArrayWithKnownFirstArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments(
                            "[{\"ὊὊὊ1\":\"ὊὊὊ1\"}, {\"ὊὊὊ2\":\"ὊὊὊO2\"}, {\"a3\":\"b3\"}, {\"ὊὊὊ4\":\"ὊὊὊ4\"}]",
                            "{\"ὊὊὊ1\":\"ὊὊὊ1\"}", 3),
                    arguments(
                            "[{\"aaa1\":\"aaa1\"}, {\"aaa2\":\"aaa2\"}, {\"a3\":\"b3\"}, {\"bbb4\":\"bbb4\"}]",
                            "{\"aaa1\":\"aaa1\"}", 2)
            );
        }
    }
}
