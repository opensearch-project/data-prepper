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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
            final String inputJsonArray, final String knownFirstPart, final int maxSize, final List<List<String>> expectedChunks, final List<Boolean> exceedsMaxSize) throws IOException {
        final int knownSingleBodySize = knownFirstPart.getBytes(Charset.defaultCharset()).length;
        final List<List<String>> chunkedBodies = objectUnderTest.parse(HttpData.ofUtf8(inputJsonArray),
                maxSize);

        assertThat(chunkedBodies, notNullValue());
        assertThat(chunkedBodies.size(), equalTo(expectedChunks.size()));

        for (int i = 0; i < expectedChunks.size(); i++) {
            final String reconstructed = chunkedBodies.get(i).stream().collect(Collectors.joining(",", "[", "]"));
            if (exceedsMaxSize.get(i)) {
                assertThat(reconstructed.getBytes(Charset.defaultCharset()).length,
                    greaterThanOrEqualTo(maxSize));
            } else {
                assertThat(reconstructed.getBytes(Charset.defaultCharset()).length,
                    lessThanOrEqualTo(maxSize));
            }
            
            for (int j = 0; j < expectedChunks.get(i).size(); j++) {
                assertThat(chunkedBodies.get(i).get(j), equalTo(expectedChunks.get(i).get(j)));
            }
        }
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
            // First test, all chunks smaller than maxSize, output has 3 lists, all smaller than maxSize
            String chunk11 = "{\"ὊὊὊ1\":\"ὊὊὊ1\"}";
            String chunk12 = "{\"ὊὊὊ2\":\"ὊὊὊO2\"}";
            String chunk13 = "{\"a3\":\"b3\"}";
            String chunk14 = "{\"ὊὊὊ4\":\"ὊὊὊ4\"}";
            // Second test, all chunks smaller than maxSize, output has 2 lists, all smaller than maxSize
            String chunk21 = "{\"aaa1\":\"aaa1\"}";
            String chunk22 = "{\"aaa2\":\"aaa2\"}";
            String chunk23 = "{\"a3\":\"b3\"}";
            String chunk24 = "{\"bbb4\":\"bbb4\"}";
            // Third test, all chunks larger than maxSize, output has 4 lists, all larger than maxSize
            String chunk31 = "{\"ὊὊὊ1\":\"ὊὊὊ01\"}";
            String chunk32 = "{\"ὊὊὊ2\":\"ὊὊὊO2\"}";
            String chunk33 = "{\"ὊὊὊ3\":\"ὊὊὊO3\"}";
            String chunk34 = "{\"ὊὊὊ4\":\"ὊὊὊO4\"}"; 
            // Fourth test, only first chunk larger than maxSize, output has 3 lists, with first chunk larger than maxSize and others smaller
            String chunk41 = "{\"aaaaaaaaaaa1\":\"aaaaaaaaaaa1\"}";
            String chunk42 = "{\"aaa2\":\"aaa2\"}";
            String chunk43 = "{\"a3\":\"b3\"}";
            String chunk44 = "{\"bbb4\":\"bbb4\"}";
            // Fifth test, only second chunk larger than maxSize, output has 3 lists, with second chunk larger than maxSize and others smaller
            String chunk51 = "{\"aaa2\":\"aaa2\"}";
            String chunk52 = "{\"aaaaaaaaaaa1\":\"aaaaaaaaaaa1\"}";
            String chunk53 = "{\"a3\":\"b3\"}";
            String chunk54 = "{\"bb4\":\"bb4\"}";
            // Sixth test, only last chunk larger than maxSize, output has 3 lists, with last chunk larger than maxSize and others smaller
            String chunk61 = "{\"aaa2\":\"aaa2\"}";
            String chunk62 = "{\"a3\":\"b3\"}";
            String chunk63 = "{\"bbb4\":\"bbb4\"}";
            String chunk64 = "{\"aaaaaaaaaaa1\":\"aaaaaaaaaaa1\"}";
            final int maxSize1 = chunk11.getBytes(Charset.defaultCharset()).length * 2 + 3;
            final int maxSize2 = chunk21.getBytes(Charset.defaultCharset()).length * 2 + 3;
            final int maxSize3 = chunk31.getBytes(Charset.defaultCharset()).length - 1;
            final int maxSize4 = chunk42.getBytes(Charset.defaultCharset()).length * 2 + 3;
            final int maxSize5 = chunk51.getBytes(Charset.defaultCharset()).length * 2 + 3;
            final int maxSize6 = chunk61.getBytes(Charset.defaultCharset()).length * 2 + 3;
            return Stream.of(
                    arguments("["+chunk11+","+chunk12+","+chunk13+","+chunk14+"]", chunk11, maxSize1, List.of(List.of(chunk11), List.of(chunk12, chunk13), List.of(chunk14)), List.of(false, false, false)),
                    arguments("["+chunk21+","+chunk22+","+chunk23+","+chunk24+"]", chunk21, maxSize2, List.of(List.of(chunk21, chunk22), List.of(chunk23, chunk24)), List.of(false, false)),
                    arguments("["+chunk31+","+chunk32+","+chunk33+","+chunk34+"]", chunk31, maxSize3, List.of(List.of(chunk31), List.of(chunk32), List.of(chunk33), List.of(chunk34)), List.of(true, true, true, true)),
                    arguments("["+chunk41+","+chunk42+","+chunk43+","+chunk44+"]", chunk41, maxSize4, List.of(List.of(chunk41), List.of(chunk42, chunk43), List.of(chunk44)), List.of(true, false, false)),
                    arguments("["+chunk51+","+chunk52+","+chunk53+","+chunk54+"]", chunk51, maxSize5, List.of(List.of(chunk51), List.of(chunk52), List.of(chunk53,chunk54)), List.of(false, true, false)),
                    arguments("["+chunk61+","+chunk62+","+chunk63+","+chunk64+"]", chunk61, maxSize6, List.of(List.of(chunk61,chunk62), List.of(chunk63), List.of(chunk64)), List.of(false, false, true))
            );
        }
    }
}
