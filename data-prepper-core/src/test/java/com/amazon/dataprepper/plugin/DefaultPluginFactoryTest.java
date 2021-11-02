package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.NoPluginFoundException;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.plugins.TestSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class DefaultPluginFactoryTest {

    private PluginProviderLoader pluginProviderLoader;
    private PluginCreator pluginCreator;
    private PluginConfigurationConverter pluginConfigurationConverter;
    private Collection<PluginProvider> pluginProviders;
    private PluginProvider firstPluginProvider;
    private Class<?> baseClass;
    private String pluginName;
    private PluginSetting pluginSetting;

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
    }

    private DefaultPluginFactory createObjectUnderTest() {
        return new DefaultPluginFactory(pluginProviderLoader, pluginCreator, pluginConfigurationConverter);
    }

    @Test
    void constructor_should_throw_if_pluginProviderLoader_is_null() {
        pluginProviderLoader = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_if_pluginProviders_is_null() {
        given(pluginProviderLoader.getPluginProviders()).willReturn(null);

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_if_pluginProviders_is_empty() {
        given(pluginProviderLoader.getPluginProviders()).willReturn(Collections.emptyList());

        assertThrows(RuntimeException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_if_pluginCreator_is_null() {
        pluginCreator = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
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

            assertThrows(NoPluginFoundException.class,
                    () -> objectUnderTest.loadPlugin(baseClass, pluginSetting));

            verifyNoInteractions(pluginCreator);
        }

        @Test
        void loadPlugins_should_throw_when_no_plugin_found() {
            final DefaultPluginFactory objectUnderTest = createObjectUnderTest();

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
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(PluginArgumentsContext.class), eq(pluginName)))
                    .willReturn(expectedInstance);

            assertThat(createObjectUnderTest().loadPlugin(baseClass, pluginSetting),
                    equalTo(expectedInstance));
        }

        @Test
        void loadPlugins_should_throw_for_null_number_of_instances() {

            final DefaultPluginFactory objectUnderTest = createObjectUnderTest();
            assertThrows(IllegalArgumentException.class, () -> objectUnderTest.loadPlugins(
                    baseClass, pluginSetting, c -> null));

            verifyNoInteractions(pluginCreator);
        }

        @ParameterizedTest
        @ValueSource(ints = {-100, -2, -1})
        void loadPlugins_should_throw_for_invalid_number_of_instances(final int numberOfInstances) {

            final DefaultPluginFactory objectUnderTest = createObjectUnderTest();
            assertThrows(IllegalArgumentException.class, () -> objectUnderTest.loadPlugins(
                    baseClass, pluginSetting, c -> numberOfInstances));

            verifyNoInteractions(pluginCreator);
        }

        @Test
        void loadPlugins_should_return_an_empty_list_when_the_number_of_instances_is_0() {
            final List<?> plugins = createObjectUnderTest().loadPlugins(
                    baseClass, pluginSetting, c -> 0);

            assertThat(plugins, notNullValue());
            assertThat(plugins.size(), equalTo(0));

            verifyNoInteractions(pluginCreator);
        }

        @Test
        void loadPlugins_should_return_a_single_instance_when_the_the_numberOfInstances_is_1() {
            final TestSink expectedInstance = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(PluginArgumentsContext.class), eq(pluginName)))
                    .willReturn(expectedInstance);

            final List<?> plugins = createObjectUnderTest().loadPlugins(
                    baseClass, pluginSetting, c -> 1);

            assertThat(plugins, notNullValue());
            assertThat(plugins.size(), equalTo(1));
            assertThat(plugins.get(0), equalTo(expectedInstance));
        }

        @Test
        void loadPlugins_should_return_an_instance_for_the_total_count() {
            final TestSink expectedInstance1 = mock(TestSink.class);
            final TestSink expectedInstance2 = mock(TestSink.class);
            final TestSink expectedInstance3 = mock(TestSink.class);
            final Object convertedConfiguration = mock(Object.class);
            given(pluginConfigurationConverter.convert(PluginSetting.class, pluginSetting))
                    .willReturn(convertedConfiguration);
            given(pluginCreator.newPluginInstance(eq(expectedPluginClass), any(PluginArgumentsContext.class), eq(pluginName)))
                    .willReturn(expectedInstance1)
                    .willReturn(expectedInstance2)
                    .willReturn(expectedInstance3);

            given(pluginSetting.getNumberOfProcessWorkers())
                    .willReturn(3);

            final List<?> plugins = createObjectUnderTest().loadPlugins(
                    baseClass, pluginSetting, c -> 3);

            assertThat(plugins, notNullValue());
            assertThat(plugins.size(), equalTo(3));
            assertThat(plugins.get(0), equalTo(expectedInstance1));
            assertThat(plugins.get(1), equalTo(expectedInstance2));
            assertThat(plugins.get(2), equalTo(expectedInstance3));
        }
    }
}