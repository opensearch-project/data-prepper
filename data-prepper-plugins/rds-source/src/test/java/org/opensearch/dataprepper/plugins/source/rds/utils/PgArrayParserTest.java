package org.opensearch.dataprepper.plugins.source.rds.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PgArrayParserTest {

    @Test
    void testParseTypedArray_SimpleIntArray() {
        String input = "{1,2,3}";
        List<Integer> expected = Arrays.asList(1, 2, 3);
        Object result = PgArrayParser.parseTypedArray(input, PostgresDataType.INT4ARRAY,
                (type, value) -> Integer.parseInt(value));
        assertEquals(expected, result);
    }

    @Test
    void testParseTypedArray_NestedIntArray() {
        String input = "{{1,2},{3,4}}";
        List<List<Integer>> expected = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        );
        Object result = PgArrayParser.parseTypedArray(input, PostgresDataType.INT4ARRAY,
                (type, value) -> Integer.parseInt(value));
        assertEquals(expected, result);
    }

    @Test
    void testParseTypedArray_StringArrayWithQuotes() {
        String input = "{\"hello\",\"world\"}";
        List<String> expected = Arrays.asList("hello", "world");
        Object result = PgArrayParser.parseTypedArray(input, PostgresDataType.TEXTARRAY,
                (type, value) -> value);
        assertEquals(expected, result);
    }

    @Test
    void testParseTypedArray_EmptyArray() {
        String input = "{}";
        List<Object> expected = Collections.emptyList();
        Object result = PgArrayParser.parseTypedArray(input, PostgresDataType.INT4ARRAY,
                (type, value) -> Integer.parseInt(value));
        assertEquals(expected, result);
    }

    @Test
    void testParseTypedArray_NullElements() {
        String input = "{1,NULL,3}";
        List<Integer> expected = Arrays.asList(1, null, 3);
        Object result = PgArrayParser.parseTypedArray(input, PostgresDataType.INT4ARRAY,
                (type, value) -> value.equals("NULL") ? null : Integer.parseInt(value));
        assertEquals(expected, result);
    }

    @Test
    void testParseTypedArray_BoxArray() {
        String input = "{(0,0,1,1);(2,2,3,3)}";
        List<String> expected = Arrays.asList("(0,0,1,1)", "(2,2,3,3)");
        Object result = PgArrayParser.parseTypedArray(input, PostgresDataType.BOX,
                (type, value) -> value);
        assertEquals(expected, result);
    }

    @Test
    void testParseRawArray_SimpleArray() {
        String input = "{1,2,3}";
        List<Object> expected = Arrays.asList("1", "2", "3");
        List<Object> result = PgArrayParser.parseRawArray(input, ',');
        assertEquals(expected, result);
    }

    @Test
    void testParseRawArray_NestedArray() {
        String input = "{{1,2},{3,4}}";
        List<Object> expected = Arrays.asList(
                Arrays.asList("1", "2"),
                Arrays.asList("3", "4")
        );
        List<Object> result = PgArrayParser.parseRawArray(input, ',');
        assertEquals(expected, result);
    }

    @Test
    void testParseRawArray_WithEscapedCharacters() {
        String input = "{\"hello,world\",\"escaped\\\"quote\"}";
        List<Object> expected = Arrays.asList("hello,world", "escaped\"quote");
        List<Object> result = PgArrayParser.parseRawArray(input, ',');
        assertEquals(expected, result);
    }

    @Test
    void testParseRawArray_NullInput() {
        assertNull(PgArrayParser.parseRawArray(null, ','));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidArrayStrings")
    void testParseRawArray_InvalidInput(String input) {
        assertThrows(IllegalArgumentException.class, () -> PgArrayParser.parseRawArray(input, ','));
    }

    private static Stream<Arguments> provideInvalidArrayStrings() {
        return Stream.of(
                Arguments.of("1,2,3"),  // No curly braces
                Arguments.of("{1,2,3"), // Missing closing brace
                Arguments.of("1,2,3}")  // Missing opening brace
        );
    }
}

