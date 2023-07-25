/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

class TemplateTypeTest {
    private IndexTemplateAPIWrapper indexTemplateAPIWrapper;

    @BeforeEach
    void setUp() {
        indexTemplateAPIWrapper = mock(IndexTemplateAPIWrapper.class);
    }

    @ParameterizedTest
    @ArgumentsSource(EnumToStrategyClass.class)
    void createTemplateStrategy_returns_instance_of_expected_type(final TemplateType objectUnderTest, final Class<?> expectedStrategyClass) {
        assertThat(objectUnderTest.createTemplateStrategy(indexTemplateAPIWrapper),
                instanceOf(expectedStrategyClass));
    }

    @ParameterizedTest
    @EnumSource(TemplateType.class)
    void createTemplateStrategy_returns_for_all_enum_types(final TemplateType objectUnderTest) {
        assertThat(objectUnderTest.createTemplateStrategy(indexTemplateAPIWrapper), notNullValue());
    }

    @ParameterizedTest
    @EnumSource(TemplateType.class)
    void fromTypeName_returns_for_all_enum_types(final TemplateType templateType) {
        assertThat(TemplateType.fromTypeName(templateType.getTypeName()), equalTo(templateType));
    }

    @Test
    void fromTypeName_returns_null_for_unknown_type() {
        assertThat(TemplateType.fromTypeName(UUID.randomUUID().toString()), nullValue());
    }

    private static class EnumToStrategyClass implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(TemplateType.V1, V1TemplateStrategy.class),
                    arguments(TemplateType.INDEX_TEMPLATE, ComposableIndexTemplateStrategy.class)
            );
        }
    }
}