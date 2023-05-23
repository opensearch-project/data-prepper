/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class TemplateTypeTest {
    private OpenSearchClient openSearchClient;

    @BeforeEach
    void setUp() {
        openSearchClient = mock(OpenSearchClient.class);
    }

    @ParameterizedTest
    @ArgumentsSource(EnumToStrategyClass.class)
    void createTemplateStrategy_returns_instance_of_expected_type(final TemplateType objectUnderTest, final Class<?> expectedStrategyClass) {
        assertThat(objectUnderTest.createTemplateStrategy(openSearchClient),
                instanceOf(expectedStrategyClass));
    }

    private static class EnumToStrategyClass implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    Arguments.arguments(TemplateType.V1, V1TemplateStrategy.class)
            );
        }
    }
}