/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TransformOptionTest {
    @ParameterizedTest
    @EnumSource(TransformOption.class)
    void fromTransformName_returns_expected_value(final TransformOption transformOption) {
        assertThat(TransformOption.fromTransformName(transformOption.getTransformName()), equalTo(transformOption));
    }

    @ParameterizedTest
    @EnumSource(TransformOption.class)
    void getTransformName_returns_non_empty_null_for_all_types(final TransformOption transformOption) {
        assertThat(transformOption.getTransformName(), notNullValue());
    }

    @ParameterizedTest
    @EnumSource(value = TransformOption.class, mode = EnumSource.Mode.EXCLUDE, names = {"NONE"})
    void getTransformName_returns_non_empty_string_for_all_types_except_none(final TransformOption transformOption) {
        assertThat(transformOption.getTransformName(), notNullValue());
        assertThat(transformOption.getTransformName(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(TransformOptionToKnownName.class)
    void getTransformName_returns_expected_name(final TransformOption transformOption, final String expectedString) {
        assertThat(transformOption.getTransformName(), equalTo(expectedString));
    }

    static class TransformOptionToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(TransformOption.NONE, ""),
                    arguments(TransformOption.UPPERCASE, "uppercase"),
                    arguments(TransformOption.LOWERCASE, "lowercase"),
                    arguments(TransformOption.CAPITALIZE, "capitalize")
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(TransformationArguments.class)
    void getTransformFunction_performs_expected_transformation(final TransformOption transformOption, final String inputString, final String outputString) {
        assertThat(transformOption.getTransformFunction().apply(inputString), equalTo(outputString));
    }

    static class TransformationArguments implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(TransformOption.NONE, "hello", "hello"),
                    arguments(TransformOption.NONE, "Hello", "Hello"),
                    arguments(TransformOption.NONE, "hello world", "hello world"),
                    arguments(TransformOption.UPPERCASE, "hello", "HELLO"),
                    arguments(TransformOption.UPPERCASE, "Hello", "HELLO"),
                    arguments(TransformOption.LOWERCASE, "hello", "hello"),
                    arguments(TransformOption.LOWERCASE, "Hello", "hello"),
                    arguments(TransformOption.LOWERCASE, "HELLO", "hello"),
                    arguments(TransformOption.CAPITALIZE, "hello", "Hello"),
                    arguments(TransformOption.CAPITALIZE, "hello world", "Hello world")
            );
        }
    }
}