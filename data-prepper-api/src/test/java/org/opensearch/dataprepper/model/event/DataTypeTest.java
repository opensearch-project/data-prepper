/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
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

    @ParameterizedTest
    @EnumSource(DataType.class)
    void getTypeName_returns_non_empty_string_for_all_types(final DataType dataType) {
        assertThat(dataType.getTypeName(), notNullValue());
        assertThat(dataType.getTypeName(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(DataTypeToKnownString.class)
    void getTypeName_returns_expected_name(final DataType dataType, final String expectedString) {
        assertThat(dataType.getTypeName(), equalTo(expectedString));
    }

    private static Stream<Arguments> getSameTypeTestData() {
        int[] testArray = {1,2};
        List<Integer> testList = new ArrayList<>();
        return Stream.of(
            arguments(2, "integer", true),
            arguments("testString", "string", true),
            arguments(2L, "long", true),
            arguments(2.0, "double", true),
            arguments(BigDecimal.valueOf(2.34567), "big_decimal", true),
            arguments(true, "boolean", true),
            arguments(Map.of("k","v"), "map", true),
            arguments(testArray, "array", true),
            arguments(testList, "array", true),
            arguments(2.0, "integer", false),
            arguments(2, "string", false),
            arguments("testString", "long", false),
            arguments("testString", "double", false),
            arguments(2, "boolean", false),
            arguments(2L, "map", false),
            arguments(2, "array", false)
        );
    }

    static class DataTypeToKnownString implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(DataType.STRING, "string"),
                    arguments(DataType.BOOLEAN, "boolean"),
                    arguments(DataType.INTEGER, "integer"),
                    arguments(DataType.LONG, "long"),
                    arguments(DataType.DOUBLE, "double"),
                    arguments(DataType.BIG_DECIMAL, "big_decimal"),
                    arguments(DataType.MAP, "map"),
                    arguments(DataType.ARRAY, "array")
            );
        }
    }
}
