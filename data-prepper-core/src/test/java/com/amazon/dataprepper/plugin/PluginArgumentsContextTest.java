/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PipelineDescription;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class PluginArgumentsContextTest {

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private TestPluginConfiguration testPluginConfiguration;

    @Mock
    private BeanFactory beanFactory;

    private static class TestPluginConfiguration { }

    @Test
    void createArguments_with_unavailable_argument_should_throw() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .build();

        final Class<?>[] parameterTypes = {String.class};
        assertThrows(InvalidPluginDefinitionException.class, () -> objectUnderTest.createArguments(parameterTypes));
    }

    @Test
    void createArguments_with_single_class() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class }),
                equalTo(new Object[] { testPluginConfiguration}));
    }

    @Test
    void createArguments_with_single_class_using_bean_factory() {
        final Object mock = mock(Object.class);
        doReturn(mock).when(beanFactory).getBean(eq(Object.class));

        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withBeanFactory(beanFactory)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { Object.class }),
                equalTo(new Object[] {mock}));
    }

    @Test
    void createArguments_given_bean_not_available_with_single_class_using_bean_factory() {
        doThrow(mock(BeansException.class)).when(beanFactory).getBean((Class<Object>) any());

        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withBeanFactory(beanFactory)
                .build();

        final InvalidPluginDefinitionException throwable = assertThrows(
                InvalidPluginDefinitionException.class,
                () -> objectUnderTest.createArguments(new Class[]{Object.class})
        );
        assertTrue(throwable.getCause() instanceof BeansException);
    }

    @Test
    void createArguments_with_multiple_supplier_sources() {
        final Object mock = mock(Object.class);
        doReturn(mock).when(beanFactory).getBean(eq(Object.class));

        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withPluginConfiguration(testPluginConfiguration)
                .withBeanFactory(beanFactory)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class, PluginSetting.class, Object.class }),
                equalTo(new Object[] {testPluginConfiguration, pluginSetting, mock}));
    }

    @Test
    void createArguments_with_two_classes() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class, PluginSetting.class }),
                equalTo(new Object[] { testPluginConfiguration, pluginSetting }));
    }

    @Test
    void createArguments_with_two_classes_inverted_order() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { PluginSetting.class, TestPluginConfiguration.class }),
                equalTo(new Object[] { pluginSetting, testPluginConfiguration }));
    }

    @Test
    void createArguments_with_three_classes() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .withPipelineDescription(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class, PluginSetting.class, PipelineDescription.class}),
                equalTo(new Object[] { testPluginConfiguration, pluginSetting, pluginSetting }));
    }

    @Test
    void createArguments_with_pluginFactory_should_return_the_instance_from_the_builder() {
        final PluginFactory pluginFactory = mock(PluginFactory.class);
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withPluginFactory(pluginFactory)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { PluginFactory.class }),
                equalTo(new Object[] { pluginFactory }));
    }

    @Test
    void createArguments_with_PluginMetrics() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .build();

        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);

        final Object[] arguments;
        try(final MockedStatic<PluginMetrics> pluginMetricsMockedStatic = mockStatic(PluginMetrics.class)) {
            pluginMetricsMockedStatic.when(() -> PluginMetrics.fromPluginSetting(pluginSetting))
                    .thenReturn(pluginMetrics);
            arguments = objectUnderTest.createArguments(new Class[]{PluginSetting.class, PluginMetrics.class});
        }
        assertThat(arguments,
                equalTo(new Object[] { pluginSetting, pluginMetrics }));
    }
}