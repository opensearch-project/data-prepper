/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.plugin.PluginProvider;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BaseDataPrepperPluginStandardTestSuiteTest {

    @Mock
    private DataPrepperPluginTestContext dataPrepperPluginTestContext;

    @Mock
    private PluginProvider pluginProvider;

    private String pluginName;
    private Class<?> pluginType;

    @BeforeEach
    void setUp() {
        pluginName = UUID.randomUUID().toString();
        pluginType = Processor.class;
    }

    private BaseDataPrepperPluginStandardTestSuite createObjectUnderTest() {
        return mock(BaseDataPrepperPluginStandardTestSuite.class, CALLS_REAL_METHODS);
    }

    @Test
    void standardDataPrepperPluginTests_generate_at_least_one_test() {
        final Stream<DynamicTest> dynamicTestStream = createObjectUnderTest()
                .standardDataPrepperPluginTests(dataPrepperPluginTestContext, pluginProvider);

        assertThat(dynamicTestStream, notNullValue());
        assertThat(dynamicTestStream.count(), greaterThanOrEqualTo(1L));
    }

    @Nested
    class TestToFindPlugin {
        private Executable testExecutable;

        @BeforeEach
        void setUp() {
            when(dataPrepperPluginTestContext.getPluginType()).thenReturn((Class) pluginType);
            when(dataPrepperPluginTestContext.getPluginName()).thenReturn(pluginName);

            final Stream<DynamicTest> dynamicTestStream = createObjectUnderTest()
                    .standardDataPrepperPluginTests(dataPrepperPluginTestContext, pluginProvider);

            assertThat(dynamicTestStream, notNullValue());
            final List<DynamicTest> orderedTests = dynamicTestStream.collect(Collectors.toList());

            DynamicTest testToFindPlugin = orderedTests.get(0);

            assertThat(testToFindPlugin, notNullValue());

            testExecutable = testToFindPlugin.getExecutable();
        }

        @Test
        void test_succeeds_when_plugin_is_found() {
            when(pluginProvider.findPluginClass(pluginType, pluginName))
                    .thenReturn(Optional.of((Class) pluginType));

            assertDoesNotThrow(() -> testExecutable.execute());
        }

        @Test
        void test_fails_when_plugin_is_not_found() {
            when(pluginProvider.findPluginClass(pluginType, pluginName))
                    .thenReturn(Optional.empty());

            assertThrows(AssertionError.class, () -> testExecutable.execute());
        }
    }

    @Nested
    class TestForPluginName {
        private Executable testExecutable;

        @BeforeEach
        void setUp() {
            final Stream<DynamicTest> dynamicTestStream = createObjectUnderTest()
                    .standardDataPrepperPluginTests(dataPrepperPluginTestContext, pluginProvider);

            assertThat(dynamicTestStream, notNullValue());
            final List<DynamicTest> orderedTests = dynamicTestStream.collect(Collectors.toList());

            DynamicTest dynamicTest = orderedTests.get(1);

            assertThat(dynamicTest, notNullValue());

            testExecutable = dynamicTest.getExecutable();
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "good",
                "good1",
                "good2_abc",
                "_abc_123",
                "abc_",
                "123"
        })
        void test_succeeds_when_plugin_name_matches_expectations(final String pluginName) {
            when(dataPrepperPluginTestContext.getPluginName()).thenReturn(pluginName);

            assertDoesNotThrow(() -> testExecutable.execute());
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "bad-name",
                "badName"
        })
        void test_fails_when_plugin_name_does_not_match_expectations(final String pluginName) {
            when(dataPrepperPluginTestContext.getPluginName()).thenReturn(pluginName);

            assertThrows(AssertionError.class, () -> testExecutable.execute());
        }
    }
}
