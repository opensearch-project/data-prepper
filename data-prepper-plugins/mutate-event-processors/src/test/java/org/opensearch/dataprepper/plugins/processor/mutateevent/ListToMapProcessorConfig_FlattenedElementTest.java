/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

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

class ListToMapProcessorConfig_FlattenedElementTest {

    @ParameterizedTest
    @EnumSource(ListToMapProcessorConfig.FlattenedElement.class)
    void fromOptionValue_returns_expected_value(final ListToMapProcessorConfig.FlattenedElement flattenedElement) {
        assertThat(ListToMapProcessorConfig.FlattenedElement.fromOptionValue(flattenedElement.getOptionValue()), equalTo(flattenedElement));
    }

    @ParameterizedTest
    @EnumSource(ListToMapProcessorConfig.FlattenedElement.class)
    void getOptionValue_returns_non_empty_string_for_all_types(final ListToMapProcessorConfig.FlattenedElement flattenedElement) {
        assertThat(flattenedElement.getOptionValue(), notNullValue());
        assertThat(flattenedElement.getOptionValue(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(FlattenedElementToKnownName.class)
    void getOptionValue_returns_expected_name(final ListToMapProcessorConfig.FlattenedElement flattenedElement, final String expectedString) {
        assertThat(flattenedElement.getOptionValue(), equalTo(expectedString));
    }

    static class FlattenedElementToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(ListToMapProcessorConfig.FlattenedElement.FIRST, "first"),
                    arguments(ListToMapProcessorConfig.FlattenedElement.LAST, "last")
            );
        }
    }
}