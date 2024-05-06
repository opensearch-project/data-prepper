/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.stream.Stream;

class DataTypeTest {
    @ParameterizedTest
    @EnumSource(DataType.class)
    void fromTypeName_returns_expected_value(final DataType dataType) {
        assertThat(DataType.fromTypeName(dataType.getTypeName()), equalTo(dataType));
    }

    @Test
    void test_isSameType_withInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> DataType.isSameType(2, "unknown"));
    }

    @ParameterizedTest
    @MethodSource("getSameTypeTestData")
    void test_isSameType(Object object, String type, boolean expectedResult) {
        assertThat(DataType.isSameType(object, type), equalTo(expectedResult));
    }

    private static Stream<Arguments> getSameTypeTestData() {
        int testArray[] = {1,2};
        return Stream.of(
            Arguments.of(2, "integer", true),
            Arguments.of("testString", "string", true),
            Arguments.of(2L, "long", true),
            Arguments.of(2.0, "double", true),
            Arguments.of(true, "boolean", true),
            Arguments.of(Map.of("k","v"), "map", true),
            Arguments.of(testArray, "array", true),
            Arguments.of(2.0, "integer", false),
            Arguments.of(2, "string", false),
            Arguments.of("testString", "long", false),
            Arguments.of("testString", "double", false),
            Arguments.of(2, "boolean", false),
            Arguments.of(2L, "map", false),
            Arguments.of(2, "array", false)
        );
    }
}
