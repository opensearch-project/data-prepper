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
import org.opensearch.dataprepper.model.event.DataType;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TargetTypeTest {
    @ParameterizedTest
    @EnumSource(TargetType.class)
    void fromTypeName_returns_expected_value(final TargetType targetType) {
        assertThat(TargetType.fromOptionValue(targetType.getDataType().getTypeName()), equalTo(targetType));
    }
    @ParameterizedTest
    @ArgumentsSource(DataTypeToTargetTypeArgumentsProvider.class)
    void fromTypeName_returns_expected_value_based_on_DataType(final String typeName, final TargetType targetType) {
        assertThat(TargetType.fromOptionValue(typeName), equalTo(targetType));
    }

    static class DataTypeToTargetTypeArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(DataType.STRING.getTypeName(), TargetType.STRING),
                    arguments(DataType.BOOLEAN.getTypeName(), TargetType.BOOLEAN),
                    arguments(DataType.INTEGER.getTypeName(), TargetType.INTEGER),
                    arguments(DataType.LONG.getTypeName(), TargetType.LONG),
                    arguments(DataType.DOUBLE.getTypeName(), TargetType.DOUBLE),
                    arguments(DataType.BIG_DECIMAL.getTypeName(), TargetType.BIG_DECIMAL)
            );
        }
    }
}