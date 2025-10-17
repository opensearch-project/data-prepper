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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugin.ClasspathPluginProvider;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;

import java.lang.reflect.Parameter;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperPluginTestFrameworkExtensionTest {
    @Mock
    private ParameterContext parameterContext;

    @Mock
    private ExtensionContext extensionContext;

    @Mock
    private Parameter parameter;

    @BeforeEach
    void setUp() {
        when(parameterContext.getParameter()).thenReturn(parameter);
    }

    @DataPrepperPluginTest(pluginName = "test_plugin_annotated", pluginType = Processor.class)
    private static class AnnotatedClass {
    }

    private static class UnannotatedClass {
    }

    private DataPrepperPluginTestFrameworkExtension createObjectUnderTest() {
        return new DataPrepperPluginTestFrameworkExtension();
    }

    @ParameterizedTest
    @ValueSource(classes = {
            DataPrepperPluginTestContext.class,
            PluginProvider.class
    })
    void supportsParameter_returns_true_for_supported_classes(final Class<?> supportedClass) {
        when(parameter.getType()).thenReturn((Class)supportedClass);

        assertThat(createObjectUnderTest().supportsParameter(parameterContext, extensionContext), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Processor.class,
            Sink.class,
            Event.class,
            EventKey.class
    })
    void supportsParameter_returns_false_for_some_unsupported_classes(final Class<?> unsupportedClass) {
        when(parameter.getType()).thenReturn((Class)unsupportedClass);

        assertThat(createObjectUnderTest().supportsParameter(parameterContext, extensionContext), equalTo(false));
    }

    @Nested
    class ResolveParameterWithDataPrepperPluginTestContext {
        @BeforeEach
        void setUp() {
            when(parameter.getType()).thenReturn((Class) DataPrepperPluginTestContext.class);
        }

        @Test
        void resolveParameter_for_DataPrepperPluginTestContext_with_annotated_class_returns_context() {
            when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedClass.class);

            final Object resolvedParameter = createObjectUnderTest().resolveParameter(parameterContext, extensionContext);

            assertThat(resolvedParameter, instanceOf(DataPrepperPluginTestContext.class));

            final DataPrepperPluginTestContext actualTestContext = (DataPrepperPluginTestContext) resolvedParameter;

            assertThat(actualTestContext, notNullValue());
            assertThat(actualTestContext.getPluginName(), equalTo("test_plugin_annotated"));
            assertThat(actualTestContext.getPluginType(), equalTo(Processor.class));
        }

        @Test
        void resolveParameter_for_DataPrepperPluginTestContext_with_unannotated_class_throws() {
            when(extensionContext.getRequiredTestClass()).thenReturn((Class) UnannotatedClass.class);

            final DataPrepperPluginTestFrameworkExtension objectUnderTest = createObjectUnderTest();

            final ParameterResolutionException actualException = assertThrows(ParameterResolutionException.class, () -> objectUnderTest.resolveParameter(parameterContext, extensionContext));

            assertThat(actualException.getMessage(), containsString("@DataPrepperPluginTest"));
            assertThat(actualException.getMessage(), containsString(UnannotatedClass.class.getName()));
        }
    }

    @Test
    void resolveParameter_for_PluginProvider() {
        when(parameter.getType()).thenReturn((Class) PluginProvider.class);
        final Object resolvedParameter = createObjectUnderTest().resolveParameter(parameterContext, extensionContext);

        assertThat(resolvedParameter, instanceOf(PluginProvider.class));
        assertThat(resolvedParameter, instanceOf(ClasspathPluginProvider.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            Processor.class,
            Sink.class,
            Event.class,
            EventKey.class
    })
    void resolveParameter_for_unsupported_type(final Class<?> unsupportedClass) {
        when(parameter.getType()).thenReturn((Class) unsupportedClass);
        final DataPrepperPluginTestFrameworkExtension objectUnderTest = createObjectUnderTest();

        final ParameterResolutionException actualException = assertThrows(ParameterResolutionException.class, () -> objectUnderTest.resolveParameter(parameterContext, extensionContext));

        assertThat(actualException.getMessage(), containsString(unsupportedClass.getName()));
    }
}