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
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class JsonCodecTest {
    private static final HttpData GOOD_TEST_DATA = HttpData.ofUtf8("[{\"a\":\"b\"}, {\"c\":\"d\"}]");
    private static final HttpData GOOD_LARGE_TEST_DATA = HttpData.ofUtf8("[{\"a1\":\"b1\"}, {\"a2\":\"b2\"}, {\"a3\":\"b3\"}, {\"a4\":\"b4\"}, {\"a5\":\"b5\"}]");
    private static final HttpData GOOD_LARGE_TEST_DATA_UNICODE = HttpData.ofUtf8("[{\"ὊὊὊ1\":\"ὊὊὊ1\"}, {\"ὊὊὊ2\":\"ὊὊὊ2\"}, {\"a3\":\"b3\"}, {\"ὊὊὊ4\":\"ὊὊὊ4\"}]");
    private final HttpData badTestDataJsonLine = HttpData.ofUtf8("{\"a\":\"b\"}");
    private final HttpData badTestDataMultiJsonLines = HttpData.ofUtf8("{\"a\":\"b\"}{\"c\":\"d\"}");
    private final HttpData badTestDataNonJson = HttpData.ofUtf8("non json content");
    private final JsonCodec objectUnderTest = new JsonCodec();

    @Test
    public void testParseSuccess() throws IOException {
        List<String> res = objectUnderTest.parse(GOOD_TEST_DATA);

        // Then
        assertEquals(2, res.size());
        assertEquals("{\"a\":\"b\"}", res.get(0));
        assertEquals("{\"c\":\"d\"}", res.get(1));
    }

    @Test
    public void testParseSuccessWithMaxSize() throws IOException {
        // When
        List<String> res = objectUnderTest.parse(GOOD_LARGE_TEST_DATA);

        assertEquals(5, res.size());

        // Then
        assertEquals("{\"a1\":\"b1\"}", res.get(0));
        assertEquals("{\"a2\":\"b2\"}", res.get(1));
        assertEquals("{\"a3\":\"b3\"}", res.get(2));
        assertEquals("{\"a4\":\"b4\"}", res.get(3));
        assertEquals("{\"a5\":\"b5\"}", res.get(4));
    }


    @ParameterizedTest
    @ValueSource(ints = {-1, -2, Integer.MIN_VALUE})
    void serializeSplit_with_invalid_splitLength(final int splitLength) {
        final Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.serializeSplit(GOOD_LARGE_TEST_DATA, serializedBodyConsumer, splitLength));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 24})
    void serializeSplit_with_split_length_leading_to_groups_of_one(final int splitLength) throws IOException {
        final Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        objectUnderTest.serializeSplit(GOOD_LARGE_TEST_DATA, serializedBodyConsumer, splitLength);

        final ArgumentCaptor<String> actualSerializedBodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(serializedBodyConsumer, times(5)).accept(actualSerializedBodyCaptor.capture());

        final List<String> allActualSerializedBodies = actualSerializedBodyCaptor.getAllValues();
        assertThat(allActualSerializedBodies.size(), equalTo(5));
        assertThat(allActualSerializedBodies.get(0), equalTo("[{\"a1\":\"b1\"}]"));
        assertThat(allActualSerializedBodies.get(1), equalTo("[{\"a2\":\"b2\"}]"));
        assertThat(allActualSerializedBodies.get(2), equalTo("[{\"a3\":\"b3\"}]"));
        assertThat(allActualSerializedBodies.get(3), equalTo("[{\"a4\":\"b4\"}]"));
        assertThat(allActualSerializedBodies.get(4), equalTo("[{\"a5\":\"b5\"}]"));
    }

    @ParameterizedTest
    @ValueSource(ints = {25, 30, 36})
    void serializeSplit_with_split_length_leading_to_groups_of_two(final int splitLength) throws IOException {
        final Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        objectUnderTest.serializeSplit(GOOD_LARGE_TEST_DATA, serializedBodyConsumer, splitLength);

        final ArgumentCaptor<String> actualSerializedBodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(serializedBodyConsumer, times(3)).accept(actualSerializedBodyCaptor.capture());

        final List<String> allActualSerializedBodies = actualSerializedBodyCaptor.getAllValues();
        assertThat(allActualSerializedBodies.size(), equalTo(3));
        assertThat(allActualSerializedBodies.get(0), equalTo("[{\"a1\":\"b1\"},{\"a2\":\"b2\"}]"));
        assertThat(allActualSerializedBodies.get(1), equalTo("[{\"a3\":\"b3\"},{\"a4\":\"b4\"}]"));
        assertThat(allActualSerializedBodies.get(2), equalTo("[{\"a5\":\"b5\"}]"));

        assertThat(allActualSerializedBodies.get(0).getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(splitLength));
        assertThat(allActualSerializedBodies.get(1).getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(splitLength));
        assertThat(allActualSerializedBodies.get(2).getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(splitLength));
    }

    @ParameterizedTest
    @ValueSource(ints = {37, 48})
    void serializeSplit_with_split_length_leading_to_groups_up_to_three(final int splitLength) throws IOException {
        final Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        objectUnderTest.serializeSplit(GOOD_LARGE_TEST_DATA, serializedBodyConsumer, splitLength);

        final ArgumentCaptor<String> actualSerializedBodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(serializedBodyConsumer, times(2)).accept(actualSerializedBodyCaptor.capture());

        final List<String> allActualSerializedBodies = actualSerializedBodyCaptor.getAllValues();
        assertThat(allActualSerializedBodies.size(), equalTo(2));
        assertThat(allActualSerializedBodies.get(0), equalTo("[{\"a1\":\"b1\"},{\"a2\":\"b2\"},{\"a3\":\"b3\"}]"));
        assertThat(allActualSerializedBodies.get(1), equalTo("[{\"a4\":\"b4\"},{\"a5\":\"b5\"}]"));

        assertThat(allActualSerializedBodies.get(0).getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(splitLength));
        assertThat(allActualSerializedBodies.get(1).getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(splitLength));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, Integer.MAX_VALUE})
    void serializeSplit_with_split_size_that_does_not_split(final int splitLength) throws IOException {
        final Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        objectUnderTest.serializeSplit(GOOD_LARGE_TEST_DATA, serializedBodyConsumer, splitLength);

        final ArgumentCaptor<String> actualSerializedBodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(serializedBodyConsumer, times(1)).accept(actualSerializedBodyCaptor.capture());

        final String actualSerializedBody = actualSerializedBodyCaptor.getValue();
        assertThat(actualSerializedBody, equalTo("[{\"a1\":\"b1\"},{\"a2\":\"b2\"},{\"a3\":\"b3\"},{\"a4\":\"b4\"},{\"a5\":\"b5\"}]"));
    }

    @ParameterizedTest
    @ValueSource(ints = {58, 68})
    void serializeSplit_with_split_length_unicode(final int splitLength) throws IOException {
        final Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        objectUnderTest.serializeSplit(GOOD_LARGE_TEST_DATA_UNICODE, serializedBodyConsumer, splitLength);

        final ArgumentCaptor<String> actualSerializedBodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(serializedBodyConsumer, times(2)).accept(actualSerializedBodyCaptor.capture());

        final List<String> allActualSerializedBodies = actualSerializedBodyCaptor.getAllValues();
        assertThat(allActualSerializedBodies.size(), equalTo(2));
        assertThat(allActualSerializedBodies.get(0), equalTo("[{\"ὊὊὊ1\":\"ὊὊὊ1\"},{\"ὊὊὊ2\":\"ὊὊὊ2\"}]"));
        assertThat(allActualSerializedBodies.get(1), equalTo("[{\"a3\":\"b3\"},{\"ὊὊὊ4\":\"ὊὊὊ4\"}]"));

        assertThat(allActualSerializedBodies.get(0).getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(splitLength));
        assertThat(allActualSerializedBodies.get(1).getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(splitLength));
    }

    @ParameterizedTest
    @ArgumentsSource(GoodTestData.class)
    void serializeSplit_and_parse_symmetry(final HttpData httpData) throws IOException {
        final List<String> parsedFromOriginal = objectUnderTest.parse(httpData);
        final Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        objectUnderTest.serializeSplit(httpData, serializedBodyConsumer, Integer.MAX_VALUE);
        final ArgumentCaptor<String> actualSerializedBodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(serializedBodyConsumer, times(1)).accept(actualSerializedBodyCaptor.capture());
        final String actualString = actualSerializedBodyCaptor.getValue();

        final String expectedJsonString = httpData.toStringUtf8().replace(" ", "");
        assertThat(actualString, equalTo(expectedJsonString));

        final List<String> parsedFromRewritten = objectUnderTest.parse(HttpData.ofUtf8(actualString));
        assertThat(parsedFromRewritten, equalTo(parsedFromOriginal));
    }


    @ParameterizedTest
    @ArgumentsSource(JsonArrayWithKnownFirstArgumentsProvider.class)
    public void parse_should_return_lists_smaller_than_provided_length(
            final String inputJsonArray, final String knownFirstPart, final int maxSize, final List<List<String>> expectedChunks, final List<Boolean> exceedsMaxSize) throws IOException {
        Consumer<String> serializedBodyConsumer = mock(Consumer.class);
        objectUnderTest.serializeSplit(HttpData.ofUtf8(inputJsonArray), serializedBodyConsumer, maxSize);
        ArgumentCaptor<String> actualSerializedBodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(serializedBodyConsumer, times(expectedChunks.size())).accept(actualSerializedBodyCaptor.capture());

        List<String> chunkedBodies = actualSerializedBodyCaptor.getAllValues();

        assertThat(chunkedBodies, notNullValue());
        assertThat(chunkedBodies.size(), equalTo(expectedChunks.size()));

        for (int i = 0; i < expectedChunks.size(); i++) {
            final String reconstructed = chunkedBodies.get(i);
            if (exceedsMaxSize.get(i)) {
                assertThat(reconstructed.getBytes(Charset.defaultCharset()).length,
                        greaterThanOrEqualTo(maxSize));
            } else {
                assertThat(reconstructed.getBytes(Charset.defaultCharset()).length,
                        lessThanOrEqualTo(maxSize));
            }

            List<String> reParsedToCompare = objectUnderTest.parse(HttpData.ofUtf8(reconstructed));

            for (int j = 0; j < expectedChunks.get(i).size(); j++) {
                assertThat(reParsedToCompare.get(j), equalTo(expectedChunks.get(i).get(j)));
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

    static class GoodTestData implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(GOOD_TEST_DATA),
                    arguments(GOOD_LARGE_TEST_DATA),
                    arguments(GOOD_LARGE_TEST_DATA_UNICODE)
            );
        }
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
                    arguments("[" + chunk11 + "," + chunk12 + "," + chunk13 + "," + chunk14 + "]", chunk11, maxSize1, List.of(List.of(chunk11), List.of(chunk12, chunk13), List.of(chunk14)), List.of(false, false, false)),
                    arguments("[" + chunk21 + "," + chunk22 + "," + chunk23 + "," + chunk24 + "]", chunk21, maxSize2, List.of(List.of(chunk21, chunk22), List.of(chunk23, chunk24)), List.of(false, false)),
                    arguments("[" + chunk31 + "," + chunk32 + "," + chunk33 + "," + chunk34 + "]", chunk31, maxSize3, List.of(List.of(chunk31), List.of(chunk32), List.of(chunk33), List.of(chunk34)), List.of(true, true, true, true)),
                    arguments("[" + chunk41 + "," + chunk42 + "," + chunk43 + "," + chunk44 + "]", chunk41, maxSize4, List.of(List.of(chunk41), List.of(chunk42, chunk43), List.of(chunk44)), List.of(true, false, false)),
                    arguments("[" + chunk51 + "," + chunk52 + "," + chunk53 + "," + chunk54 + "]", chunk51, maxSize5, List.of(List.of(chunk51), List.of(chunk52), List.of(chunk53, chunk54)), List.of(false, true, false)),
                    arguments("[" + chunk61 + "," + chunk62 + "," + chunk63 + "," + chunk64 + "]", chunk61, maxSize6, List.of(List.of(chunk61, chunk62), List.of(chunk63), List.of(chunk64)), List.of(false, false, true))
            );
        }
    }


    @ParameterizedTest
    @ArgumentsSource(GoodTestData.class)
    void validate_with_known_good_Json(final HttpData httpData) throws IOException {
        objectUnderTest.validate(httpData);
    }

    @Test
    void validate_with_valid_JSON_but_not_array_should_throw() {
        assertThrows(IOException.class, () -> objectUnderTest.validate(badTestDataJsonLine));
    }

    @Test
    void validate_with_multiline_JSON_should_throw() {
        assertThrows(IOException.class, () -> objectUnderTest.validate(badTestDataMultiJsonLines));
    }

    @Test
    void validate_with_invalid_JSON_should_throw() {
        assertThrows(IOException.class, () -> objectUnderTest.validate(badTestDataNonJson));
    }
}
