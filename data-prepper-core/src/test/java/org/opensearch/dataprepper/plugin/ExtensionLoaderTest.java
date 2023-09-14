/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.apache.commons.lang3.stream.Streams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.opensearch.dataprepper.plugins.test.TestExtensionConfig;
import org.opensearch.dataprepper.plugins.test.TestExtensionWithConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtensionLoaderTest {
    @Mock
    private ExtensionPluginConfigurationConverter extensionPluginConfigurationConverter;
    @Mock
    private ExtensionClassProvider extensionClassProvider;
    @Mock
    private PluginCreator pluginCreator;
    @Captor
    private ArgumentCaptor<PluginArgumentsContext> pluginArgumentsContextArgumentCaptor;

    private ExtensionLoader createObjectUnderTest() {
        return new ExtensionLoader(extensionPluginConfigurationConverter, extensionClassProvider, pluginCreator);
    }

    @Test
    void loadExtensions_returns_empty_list_when_no_plugin_classes() {
        when(extensionClassProvider.loadExtensionPluginClasses()).thenReturn(Collections.emptyList());

        final List<? extends ExtensionPlugin> extensionPlugins = createObjectUnderTest().loadExtensions();

        assertThat(extensionPlugins, notNullValue());
        assertThat(extensionPlugins.size(), equalTo(0));
    }

    @Test
    void loadExtensions_returns_single_extension_for_single_plugin_class() {
        final Class<ExtensionPlugin> pluginClass = (Class<ExtensionPlugin>) mock(ExtensionPlugin.class).getClass();

        when(extensionClassProvider.loadExtensionPluginClasses()).thenReturn(Collections.singleton(pluginClass));

        final ExtensionPlugin expectedPlugin = mock(ExtensionPlugin.class);
        when(pluginCreator.newPluginInstance(
                eq(pluginClass),
                any(PluginArgumentsContext.class),
                startsWith("extension_plugin")))
                .thenReturn(expectedPlugin);

        final List<? extends ExtensionPlugin> extensionPlugins = createObjectUnderTest().loadExtensions();

        assertThat(extensionPlugins, notNullValue());
        assertThat(extensionPlugins.size(), equalTo(1));
        assertThat(extensionPlugins.get(0), equalTo(expectedPlugin));
    }

    @ParameterizedTest
    @MethodSource("validExtensionConfigs")
    void loadExtensions_returns_single_extension_with_config_for_single_plugin_class(
            final TestExtensionConfig testExtensionConfig) {
        when(extensionClassProvider.loadExtensionPluginClasses())
                .thenReturn(Collections.singleton(TestExtensionWithConfig.class));

        final TestExtensionWithConfig expectedPlugin = mock(TestExtensionWithConfig.class);
        final String expectedPluginName = "test_extension_with_config";
        when(extensionPluginConfigurationConverter.convert(eq(TestExtensionConfig.class),
                eq("/test_extension"))).thenReturn(testExtensionConfig);
        when(pluginCreator.newPluginInstance(
                eq(TestExtensionWithConfig.class),
                any(PluginArgumentsContext.class),
                eq(expectedPluginName)))
                .thenReturn(expectedPlugin);

        final List<? extends ExtensionPlugin> extensionPlugins = createObjectUnderTest().loadExtensions();

        verify(pluginCreator).newPluginInstance(eq(TestExtensionWithConfig.class),
                pluginArgumentsContextArgumentCaptor.capture(), eq(expectedPluginName));
        assertThat(pluginArgumentsContextArgumentCaptor.getValue(), instanceOf(
                ExtensionLoader.SingleConfigArgumentArgumentsContext.class));
        assertThat(extensionPlugins, notNullValue());
        assertThat(extensionPlugins.size(), equalTo(1));
        assertThat(extensionPlugins.get(0), equalTo(expectedPlugin));
    }

    @Test
    void loadExtensions_returns_multiple_extensions_for_multiple_plugin_classes() {
        final Collection<Class<? extends ExtensionPlugin>> pluginClasses = new HashSet<>();
        final Collection<ExtensionPlugin> expectedPlugins = new ArrayList<>();

        pluginClasses.add(TestExtension1.class);
        pluginClasses.add(TestExtension2.class);
        pluginClasses.add(TestExtension3.class);

        for (Class pluginClass : pluginClasses) {
            final String expectedPluginName = ExtensionLoader.classNameToPluginName(pluginClass.getSimpleName());
            final ExtensionPlugin extensionPlugin = mock((Class<ExtensionPlugin>)pluginClass);

            when(pluginCreator.newPluginInstance(
                    eq(pluginClass),
                    any(PluginArgumentsContext.class),
                    eq(expectedPluginName)))
                    .thenReturn(extensionPlugin);

            pluginClasses.add(pluginClass);
            expectedPlugins.add(extensionPlugin);
        }

        when(extensionClassProvider.loadExtensionPluginClasses()).thenReturn(pluginClasses);

        final List<ExtensionPlugin> actualPlugins = (List<ExtensionPlugin>) createObjectUnderTest().loadExtensions();

        assertThat(actualPlugins, notNullValue());
        assertThat(actualPlugins.size(), equalTo(pluginClasses.size()));
        assertThat(actualPlugins.size(), equalTo(expectedPlugins.size()));
        for (ExtensionPlugin expectedPlugin : actualPlugins) {
            assertThat(actualPlugins, hasItem(expectedPlugin));
        }
    }

    @Test
    void loadExtensions_invokes_newPluginInstance_with_PluginArgumentsContext_not_supporting_any_arguments() {
        final Class<ExtensionPlugin> pluginClass = (Class<ExtensionPlugin>) mock(ExtensionPlugin.class).getClass();

        when(extensionClassProvider.loadExtensionPluginClasses()).thenReturn(Collections.singleton(pluginClass));

        when(pluginCreator.newPluginInstance(
                any(Class.class),
                any(PluginArgumentsContext.class),
                anyString()))
                .thenReturn(mock(ExtensionPlugin.class));

        createObjectUnderTest().loadExtensions();

        final ArgumentCaptor<PluginArgumentsContext> contextArgumentCaptor =
                ArgumentCaptor.forClass(PluginArgumentsContext.class);
        verify(pluginCreator).newPluginInstance(
                eq(pluginClass),
                contextArgumentCaptor.capture(),
                anyString());

        final PluginArgumentsContext actualPluginArgumentsContext = contextArgumentCaptor.getValue();

        final Class[] inputClasses = {String.class};
        assertThrows(InvalidPluginDefinitionException.class, () -> actualPluginArgumentsContext.createArguments(inputClasses));
    }

    @Test
    void loadExtensions_invokes_newPluginInstance_with_PluginArgumentsContext_which_returns_empty_arguments_for_empty_classes() {
        final Class<ExtensionPlugin> pluginClass = (Class<ExtensionPlugin>) mock(ExtensionPlugin.class).getClass();

        when(extensionClassProvider.loadExtensionPluginClasses()).thenReturn(Collections.singleton(pluginClass));

        when(pluginCreator.newPluginInstance(
                any(Class.class),
                any(PluginArgumentsContext.class),
                anyString()))
                .thenReturn(mock(ExtensionPlugin.class));

        createObjectUnderTest().loadExtensions();

        final ArgumentCaptor<PluginArgumentsContext> contextArgumentCaptor =
                ArgumentCaptor.forClass(PluginArgumentsContext.class);
        verify(pluginCreator).newPluginInstance(
                eq(pluginClass),
                contextArgumentCaptor.capture(),
                anyString());

        final PluginArgumentsContext actualPluginArgumentsContext = contextArgumentCaptor.getValue();

        final Object[] arguments = actualPluginArgumentsContext.createArguments(new Class[]{});
        assertThat(arguments, notNullValue());
        assertThat(arguments.length, equalTo(0));
    }

    @ParameterizedTest
    @CsvSource({
            "p,p",
            "plugin,plugin",
            "Plugin,plugin",
            "CustomPlugin,custom_plugin",
            "MyCustomPlugin,my_custom_plugin",
            "MyCustomPlugin$InnerClass,my_custom_plugin_inner_class"
    })
    void classNameToPluginName_returns_name_split_by_uppercase(final String input, final String expected) {
        assertThat(ExtensionLoader.classNameToPluginName(input), equalTo(expected));
    }

    private static Stream<Arguments>  validExtensionConfigs() {
        return Streams.of(
                Arguments.of(new TestExtensionConfig()),
                null);
    }

    private interface TestExtension1 extends ExtensionPlugin {
    }
    private interface TestExtension2 extends ExtensionPlugin {
    }
    private interface TestExtension3 extends ExtensionPlugin {
    }
}