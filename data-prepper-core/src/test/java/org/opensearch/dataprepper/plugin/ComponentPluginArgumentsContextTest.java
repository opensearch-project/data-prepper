/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.opensearch.dataprepper.model.plugin.PluginConfigurationObservable;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;

import java.util.Map;

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
class ComponentPluginArgumentsContextTest {

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private TestPluginConfiguration testPluginConfiguration;

    @Mock
    private BeanFactory beanFactory;

    private static class TestPluginConfiguration { }

    private static class SubPluginSetting extends PluginSetting {
        public SubPluginSetting(final String name, final Map<String, Object> settings) {
            super(name, settings);
        }
    }

    @Test
    void createArguments_with_unavailable_argument_should_throw() {
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .build();

        final Class<?>[] parameterTypes = {String.class};
        assertThrows(InvalidPluginDefinitionException.class, () -> objectUnderTest.createArguments(parameterTypes));
    }

    @Test
    void createArguments_with_single_class() {
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class }),
                equalTo(new Object[] { testPluginConfiguration}));
    }

    @Test
    void createArguments_with_single_class_when_PluginSetting_is_inherited() {
        pluginSetting = mock(SubPluginSetting.class);
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { PluginSetting.class }),
                equalTo(new Object[] { pluginSetting}));
    }

    @Test
    void createArguments_with_single_class_using_bean_factory() {
        final Object mock = mock(Object.class);
        doReturn(mock).when(beanFactory).getBean(eq(Object.class));

        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withBeanFactory(beanFactory)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { Object.class }),
                equalTo(new Object[] {mock}));
    }

    @Test
    void createArguments_with_single_class_using_sink_context() {
        final SinkContext sinkContext = mock(SinkContext.class);

        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withSinkContext(sinkContext)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { SinkContext.class }),
                equalTo(new Object[] { sinkContext}));
    }

    @Test
    void createArguments_with_single_class_using_plugin_configuration_observable() {
        final PluginConfigurationObservable pluginConfigurationObservable = mock(PluginConfigurationObservable.class);

        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withPluginConfigurationObservable(pluginConfigurationObservable)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { PluginConfigurationObservable.class }),
                equalTo(new Object[] { pluginConfigurationObservable }));
    }

    @Test
    void createArguments_given_bean_not_available_with_single_class_using_bean_factory() {
        doThrow(mock(BeansException.class)).when(beanFactory).getBean((Class<Object>) any());

        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
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

        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withPluginConfiguration(testPluginConfiguration)
                .withBeanFactory(beanFactory)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class, PluginSetting.class, Object.class }),
                equalTo(new Object[] {testPluginConfiguration, pluginSetting, mock}));
    }

    @Test
    void createArguments_with_two_classes() {
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class, PluginSetting.class }),
                equalTo(new Object[] { testPluginConfiguration, pluginSetting }));
    }

    @Test
    void createArguments_with_two_classes_inverted_order() {
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { PluginSetting.class, TestPluginConfiguration.class }),
                equalTo(new Object[] { pluginSetting, testPluginConfiguration }));
    }

    @Test
    void createArguments_with_three_classes() {
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
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
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withPluginFactory(pluginFactory)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { PluginFactory.class }),
                equalTo(new Object[] { pluginFactory }));
    }

    @Test
    void createArguments_with_PluginMetrics() {
        final ComponentPluginArgumentsContext objectUnderTest = new ComponentPluginArgumentsContext.Builder()
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
