/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.processor.keyvalue.KeyValueProcessorConfig.FIELD_DELIMITER_REGEX_KEY;
import static org.opensearch.dataprepper.plugins.processor.keyvalue.KeyValueProcessorConfig.KEY_VALUE_DELIMITER_REGEX_KEY;

class KeyValueProcessorConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KeyValueProcessorConfig createObjectUnderTest(final String keyValueDelimiterRegex,
                                                          final String deleteKeyRegex,
                                                          final String deleteValueRegex,
                                                          final String fieldDelimiterRegex) {
        final Map<String, Object> map = new HashMap<>();
        map.put(KEY_VALUE_DELIMITER_REGEX_KEY, keyValueDelimiterRegex);
        map.put("delete_key_regex", deleteKeyRegex);
        map.put("delete_value_regex", deleteValueRegex);
        map.put(FIELD_DELIMITER_REGEX_KEY, fieldDelimiterRegex);
        return OBJECT_MAPPER.convertValue(map, KeyValueProcessorConfig.class);
    }

    @ParameterizedTest
    @MethodSource("provideRegexAndIsValid")
    void testIsKeyValueDelimiterRegexValid(final String keyValueDelimiterRegex, final boolean isValid) {
        final KeyValueProcessorConfig objectUnderTest = createObjectUnderTest(
                keyValueDelimiterRegex, null, null, null);
        assertThat(objectUnderTest.isKeyValueDelimiterRegexValid(), is(isValid));
    }

    @ParameterizedTest
    @MethodSource("provideRegexAndIsValid")
    void testIsDeleteKeyRegexValid(final String deleteKeyRegex, final boolean isValid) {
        final KeyValueProcessorConfig objectUnderTest = createObjectUnderTest(
                null, deleteKeyRegex, null, null);
        assertThat(objectUnderTest.isDeleteKeyRegexValid(), is(isValid));
    }

    @ParameterizedTest
    @MethodSource("provideRegexAndIsValid")
    void testIsDeleteValueRegexValid(final String deleteValueRegex, final boolean isValid) {
        final KeyValueProcessorConfig objectUnderTest = createObjectUnderTest(
                null, null, deleteValueRegex, null);
        assertThat(objectUnderTest.isDeleteValueRegexValid(), is(isValid));
    }

    @ParameterizedTest
    @MethodSource("provideRegexAndIsValid")
    void testIsFieldDelimiterRegexValid(final String fieldDelimiterRegex, final boolean isValid) {
        final KeyValueProcessorConfig objectUnderTest = createObjectUnderTest(
                null, null, null, fieldDelimiterRegex);
        assertThat(objectUnderTest.isFieldDelimiterRegexValid(), is(isValid));
    }

    private static Stream<Arguments> provideRegexAndIsValid() {
        return Stream.of(
                Arguments.of(null, true),
                Arguments.of("", true),
                Arguments.of("abc", true),
                Arguments.of("(abc", false)
        );
    }
}