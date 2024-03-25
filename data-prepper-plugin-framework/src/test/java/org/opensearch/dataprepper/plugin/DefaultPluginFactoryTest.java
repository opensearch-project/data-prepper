/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.NoPluginFoundException;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.test.TestSink;
import org.springframework.beans.factory.BeanFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DefaultPluginFactoryTest {

    private PluginProviderLoader pluginProviderLoader;
    private PluginCreator pluginCreator;
    private PluginConfigurationConverter pluginConfigurationConverter;
    private Collection<PluginProvider> pluginProviders;
    private PluginProvider firstPluginProvider;
    private Class<?> baseClass;
    private String pluginName;
    private PluginSetting pluginSetting;
    private PluginBeanFactoryProvider beanFactoryProvider;
    private BeanFactory beanFactory;
    private String pipelineName;
    private PluginConfigurationObservableFactory pluginConfigurationObservableFactory;
    private PluginConfigObservable pluginConfigObservable;
    private ApplicationContextToTypedSuppliers applicationContextToTypedSuppliers;

    @BeforeEach
    void setUp() {
        pluginProviderLoader = mock(PluginProviderLoader.class);
        pluginCreator = mock(PluginCreator.class);
        pluginConfigurationConverter = mock(PluginConfigurationConverter.class);
        pluginProviders = new ArrayList<>();
        given(pluginProviderLoader.getPluginProviders()).willReturn(pluginProviders);
        firstPluginProvider = mock(PluginProvider.class);
        pluginProviders.add(firstPluginProvider);

        baseClass = Sink.class;
        pluginName = UUID.randomUUID().toString();
        pluginSetting = mock(PluginSetting.class);
        given(pluginSetting.getName()).willReturn(pluginName);
        pipelineName = UUID.randomUUID().toString();
        given(pluginSetting.getPipelineName()).willReturn(pipelineName);

        beanFactoryProvider = mock(PluginBeanFactoryProvider.class);
        beanFactory = mock(BeanFactory.class);
        pluginConfigurationObservableFactory = mock(PluginConfigurationObservableFactory.class);
        given(pluginConfigurationObservableFactory.createDefaultPluginConfigObservable(
                eq(pluginConfigurationConverter),
                any(Class.class),
                any(PluginSetting.class)
        )).willReturn(pluginConfigObservable);

        applicationContextToTypedSuppliers = mock(ApplicationContextToTypedSuppliers.class);
    }

    private DefaultPluginFactory createObjectUnderTest() {
        return new DefaultPluginFactory(
                pluginProviderLoader, pluginCreator, pluginConfigurationConverter,
                beanFactoryProvider,
                pluginConfigurationObservableFactory,
                applicationContextToTypedSuppliers);
    }

    @Test
    void constructor_should_throw_if_pluginProviderLoader_is_null() {
        pluginProviderLoader = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
        verifyNoInteractions(beanFactoryProvider);
    }

    @Test
    void constructor_should_throw_if_pluginProviders_is_null() {
        given(pluginProviderLoader.getPluginProviders()).willReturn(null);

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
        verifyNoInteractions(beanFactoryProvider);
    }

    @Test
    void constructor_should_throw_if_pluginProviders_is_empty() {
        given(pluginProviderLoader.getPluginProviders()).willReturn(Collections.emptyList());

        assertThrows(RuntimeException.class,
                this::createObjectUnderTest);
        verifyNoInteractions(beanFactoryProvider);
    }

    @Test
    void constructor_should_throw_if_pluginConfigurationObservableFactory_is_null() {
        pluginConfigurationObservableFactory = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
        verifyNoInteractions(beanFactoryProvider);
    }

    @Test
    void constructor_should_throw_if_pluginCreator_is_null() {
        pluginCreator = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
        verifyNoInteractions(beanFactoryProvider);
    }

    @Nested
    class WithoutPlugin {

        @BeforeEach
        void setUp() {
            given(firstPluginProvider.findPluginClass(baseClass, pluginName))
                    .willReturn(Optional.empty());
        }

        @Test
        void loadPlugin_should_throw_if_no_plugin_found() {
            final DefaultPluginFactory objectUnderTest = createObjectUnderTest();

            verifyNoInteractions(beanFactoryProvider);
            assertThrows(NoPluginFoundException.class,
                    () -> objectUnderTest.loadPlugin(baseClass, pluginSetting));

            verifyNoInteractions(pluginCreator);
        }

        @Test
        void loadPlugins_should_throw_when_no_plugin_found() {
            final DefaultPluginFactory objectUnderTest = createObjectUnderTest();

            verifyNoInteractions(beanFactoryProvider);
            assertThrows(NoPluginFoundException.class,
                    () -> objectUnderTest.loadPlugins(baseClass, pluginSetting,
                            c -> 1));

            verifyNoInteractions(pluginCreator);
        }
    }

    @Nested
    class WithFoundPlugin {

        @SuppressWarnings("rawtypes")
        private Class expectedPluginClass;

        @BeforeEach
        void setUp() {
            expectedPluginClass = TestSink.class;

            given(firstPluginProvider.findPluginClass(baseClass, pluginName))
                    .willReturn(Optional.of(expectedPluginClass));
        }

        @Test
        void loadPlugin_should_create_a_new_instance_of_the_first_plugin_found() {

            final TestSink expectedInstance = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(ComponentPluginArgumentsContext.class), eq(pluginName)))
                    .willReturn(expectedInstance);

            assertThat(createObjectUnderTest().loadPlugin(baseClass, pluginSetting),
                    equalTo(expectedInstance));
            verify(pluginConfigurationObservableFactory).createDefaultPluginConfigObservable(eq(pluginConfigurationConverter),
                    eq(PluginSetting.class), eq(pluginSetting));
            verify(beanFactoryProvider).get();
        }

        @Test
        void loadPlugins_should_throw_for_null_number_of_instances() {

            final DefaultPluginFactory objectUnderTest = createObjectUnderTest();
            assertThrows(IllegalArgumentException.class, () -> objectUnderTest.loadPlugins(
                    baseClass, pluginSetting, c -> null));

            verifyNoInteractions(beanFactoryProvider);
            verifyNoInteractions(pluginCreator);
        }

        @ParameterizedTest
        @ValueSource(ints = {-100, -2, -1})
        void loadPlugins_should_throw_for_invalid_number_of_instances(final int numberOfInstances) {

            final DefaultPluginFactory objectUnderTest = createObjectUnderTest();
            assertThrows(IllegalArgumentException.class, () -> objectUnderTest.loadPlugins(
                    baseClass, pluginSetting, c -> numberOfInstances));

            verifyNoInteractions(beanFactoryProvider);
            verifyNoInteractions(pluginCreator);
        }

        @Test
        void loadPlugins_should_return_an_empty_list_when_the_number_of_instances_is_0() {
            final List<?> plugins = createObjectUnderTest().loadPlugins(
                    baseClass, pluginSetting, c -> 0);

            assertThat(plugins, notNullValue());
            assertThat(plugins.size(), equalTo(0));

            verify(beanFactoryProvider).get();
            verifyNoInteractions(pluginCreator);
        }

        @Test
        void loadPlugins_should_return_a_single_instance_when_the_the_numberOfInstances_is_1() {
            final TestSink expectedInstance = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(ComponentPluginArgumentsContext.class), eq(pluginName)))
                    .willReturn(expectedInstance);

            final List<?> plugins = createObjectUnderTest().loadPlugins(
                    baseClass, pluginSetting, c -> 1);

            verify(beanFactoryProvider).get();
            verify(pluginConfigurationObservableFactory).createDefaultPluginConfigObservable(eq(pluginConfigurationConverter),
                    eq(PluginSetting.class), eq(pluginSetting));
            final ArgumentCaptor<ComponentPluginArgumentsContext> pluginArgumentsContextArgCapture = ArgumentCaptor.forClass(ComponentPluginArgumentsContext.class);
            verify(pluginCreator).newPluginInstance(eq(expectedPluginClass), pluginArgumentsContextArgCapture.capture(), eq(pluginName));
            final ComponentPluginArgumentsContext actualPluginArgumentsContext = pluginArgumentsContextArgCapture.getValue();
            final List<Class> classes = List.of(PipelineDescription.class);
            final Object[] pipelineDescriptionObj = actualPluginArgumentsContext.createArguments(classes.toArray(new Class[1]));
            assertThat(pipelineDescriptionObj.length, equalTo(1));
            assertThat(pipelineDescriptionObj[0], instanceOf(PipelineDescription.class));
            final PipelineDescription actualPipelineDescription = (PipelineDescription)pipelineDescriptionObj[0];
            assertThat(actualPipelineDescription.getPipelineName(), is(pipelineName));
            assertThat(plugins, notNullValue());
            assertThat(plugins.size(), equalTo(1));
            assertThat(plugins.get(0), equalTo(expectedInstance));
        }

        @Test
        void loadPlugin_with_varargs_should_return_a_single_instance_when_the_the_numberOfInstances_is_1() {
            final Object object = new Object();
            final TestSink expectedInstance = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(ComponentPluginArgumentsContext.class), eq(pluginName), eq(object)))
                    .willReturn(expectedInstance);

            final Object plugin = createObjectUnderTest().loadPlugin(baseClass, pluginSetting, object);

            verify(beanFactoryProvider).get();
            verify(pluginConfigurationObservableFactory).createDefaultPluginConfigObservable(eq(pluginConfigurationConverter),
                    eq(PluginSetting.class), eq(pluginSetting));
            final ArgumentCaptor<ComponentPluginArgumentsContext> pluginArgumentsContextArgCapture = ArgumentCaptor.forClass(ComponentPluginArgumentsContext.class);
            verify(pluginCreator).newPluginInstance(eq(expectedPluginClass), pluginArgumentsContextArgCapture.capture(), eq(pluginName), eq(object));
            final ComponentPluginArgumentsContext actualPluginArgumentsContext = pluginArgumentsContextArgCapture.getValue();
            final List<Class> classes = List.of(PipelineDescription.class);
            final Object[] pipelineDescriptionObj = actualPluginArgumentsContext.createArguments(classes.toArray(new Class[1]));
            assertThat(pipelineDescriptionObj.length, equalTo(1));
            assertThat(pipelineDescriptionObj[0], instanceOf(PipelineDescription.class));
            final PipelineDescription actualPipelineDescription = (PipelineDescription)pipelineDescriptionObj[0];
            assertThat(actualPipelineDescription.getPipelineName(), is(pipelineName));
            assertThat(plugin, notNullValue());
            assertThat(plugin, equalTo(expectedInstance));
        }

        @Test
        void loadPlugins_should_return_an_instance_for_the_total_count() {
            final TestSink expectedInstance1 = mock(TestSink.class);
            final TestSink expectedInstance2 = mock(TestSink.class);
            final TestSink expectedInstance3 = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(ComponentPluginArgumentsContext.class), eq(pluginName)))
                    .willReturn(expectedInstance1)
                    .willReturn(expectedInstance2)
                    .willReturn(expectedInstance3);

            given(pluginSetting.getNumberOfProcessWorkers())
                    .willReturn(3);

            final List<?> plugins = createObjectUnderTest().loadPlugins(
                    baseClass, pluginSetting, c -> 3);

            verify(beanFactoryProvider).get();
            final ArgumentCaptor<ComponentPluginArgumentsContext> pluginArgumentsContextArgCapture = ArgumentCaptor.forClass(ComponentPluginArgumentsContext.class);
            verify(pluginCreator, times(3)).newPluginInstance(eq(expectedPluginClass), pluginArgumentsContextArgCapture.capture(), eq(pluginName));
            final List<ComponentPluginArgumentsContext> actualPluginArgumentsContextList = pluginArgumentsContextArgCapture.getAllValues();
            assertThat(actualPluginArgumentsContextList.size(), equalTo(3));
            actualPluginArgumentsContextList.forEach(pluginArgumentsContext -> {
                final List<Class> classes = List.of(PipelineDescription.class);
                final Object[] pipelineDescriptionObj = pluginArgumentsContext.createArguments(classes.toArray(new Class[1]));
                assertThat(pipelineDescriptionObj.length, equalTo(1));
                assertThat(pipelineDescriptionObj[0], instanceOf(PipelineDescription.class));
                final PipelineDescription actualPipelineDescription = (PipelineDescription)pipelineDescriptionObj[0];
                assertThat(actualPipelineDescription.getPipelineName(), is(pipelineName));
            });
            assertThat(plugins, notNullValue());
            assertThat(plugins.size(), equalTo(3));
            assertThat(plugins.get(0), equalTo(expectedInstance1));
            assertThat(plugins.get(1), equalTo(expectedInstance2));
            assertThat(plugins.get(2), equalTo(expectedInstance3));
        }


        @Test
        void loadPlugins_should_return_a_single_instance_with_values_from_ApplicationContextToTypedSuppliers() {
            final TestSink expectedInstance = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            final String suppliedAdditionalArgument = UUID.randomUUID().toString();
            Map<Class<?>, Supplier<Object>> additionalArgumentsSuppliers = Map.of(String.class, () -> suppliedAdditionalArgument);
            when(applicationContextToTypedSuppliers.getArgumentsSuppliers()).thenReturn(additionalArgumentsSuppliers);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(ComponentPluginArgumentsContext.class), eq(pluginName)))
                    .willReturn(expectedInstance);

            final List<?> plugins = createObjectUnderTest().loadPlugins(
                    baseClass, pluginSetting, c -> 1);

            verify(beanFactoryProvider).get();
            final ArgumentCaptor<ComponentPluginArgumentsContext> pluginArgumentsContextArgCapture = ArgumentCaptor.forClass(ComponentPluginArgumentsContext.class);
            verify(pluginCreator).newPluginInstance(eq(expectedPluginClass), pluginArgumentsContextArgCapture.capture(), eq(pluginName));
            final ComponentPluginArgumentsContext actualPluginArgumentsContext = pluginArgumentsContextArgCapture.getValue();
            final List<Class> classes = List.of(PipelineDescription.class, String.class);
            final Object[] pipelineDescriptionObj = actualPluginArgumentsContext.createArguments(classes.toArray(new Class[2]));
            assertThat(pipelineDescriptionObj.length, equalTo(2));
            assertThat(pipelineDescriptionObj[0], instanceOf(PipelineDescription.class));
            assertThat(pipelineDescriptionObj[1], sameInstance(suppliedAdditionalArgument));
            final PipelineDescription actualPipelineDescription = (PipelineDescription)pipelineDescriptionObj[0];
            assertThat(actualPipelineDescription.getPipelineName(), is(pipelineName));
            assertThat(plugins, notNullValue());
            assertThat(plugins.size(), equalTo(1));
            assertThat(plugins.get(0), equalTo(expectedInstance));
        }
    }

    @Nested
    class WithFoundDeprecatedPluginName {
        private static final String TEST_SINK_DEPRECATED_NAME = "test_sink_deprecated_name";
        private Class expectedPluginClass;

        @BeforeEach
        void setUp() {
            expectedPluginClass = TestSink.class;
            given(pluginSetting.getName()).willReturn(TEST_SINK_DEPRECATED_NAME);

            given(firstPluginProvider.findPluginClass(baseClass, TEST_SINK_DEPRECATED_NAME))
                    .willReturn(Optional.of(expectedPluginClass));
        }

        @Test
        void loadPlugin_should_create_a_new_instance_of_the_first_plugin_found_with_correct_name_and_deprecated_name() {
            final TestSink expectedInstance = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(ComponentPluginArgumentsContext.class), eq(TEST_SINK_DEPRECATED_NAME)))
                    .willReturn(expectedInstance);

            assertThat(createObjectUnderTest().loadPlugin(baseClass, pluginSetting), equalTo(expectedInstance));
            MatcherAssert.assertThat(expectedInstance.getClass().getAnnotation(DataPrepperPlugin.class).deprecatedName(), equalTo(TEST_SINK_DEPRECATED_NAME));
            verify(beanFactoryProvider).get();
        }
    }
}
