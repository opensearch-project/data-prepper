/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
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

import java.lang.reflect.Parameter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PluginProviderParameterResolverTest {
    @Mock
    private ParameterContext parameterContext;

    @Mock
    private ExtensionContext extensionContext;

    @Mock
    private Parameter parameter;

    private PluginProviderParameterResolver createObjectUnderTest() {
        return new PluginProviderParameterResolver();
    }

    @Test
    void supportsParameter_returns_true_for_supported_class() {
        when(parameterContext.getParameter()).thenReturn(parameter);
        when(parameter.getType()).thenReturn((Class)PluginProvider.class);

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
        when(parameterContext.getParameter()).thenReturn(parameter);
        when(parameter.getType()).thenReturn((Class)unsupportedClass);

        assertThat(createObjectUnderTest().supportsParameter(parameterContext, extensionContext), equalTo(false));
    }

    @Test
    void resolveParameter_for_PluginProvider() {
        final Object resolvedParameter = createObjectUnderTest().resolveParameter(parameterContext, extensionContext);

        assertThat(resolvedParameter, instanceOf(PluginProvider.class));
        assertThat(resolvedParameter, instanceOf(ClasspathPluginProvider.class));
    }
}