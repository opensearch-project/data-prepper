/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.validation.PluginError;
import org.opensearch.dataprepper.validation.PluginErrorsHandler;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtensionsApplierTest {
    @Mock
    private DataPrepperExtensionPoints dataPrepperExtensionPoints;
    @Mock
    private ExtensionLoader extensionLoader;
    @Mock
    private PluginErrorCollector pluginErrorCollector;
    @Mock
    private PluginErrorsHandler pluginErrorsHandler;

    private ExtensionsApplier createObjectUnderTest() {
        return new ExtensionsApplier(
                dataPrepperExtensionPoints, extensionLoader, pluginErrorCollector, pluginErrorsHandler);
    }

    @AfterEach
    void extensionPointsHasNoInteractions() {
        verifyNoInteractions(dataPrepperExtensionPoints);
    }

    @Test
    void applyExtensions_calls_apply_on_all_loaded_extensions() {
        final List<ExtensionPlugin> extensionPlugins = List.of(mock(ExtensionPlugin.class), mock(ExtensionPlugin.class), mock(ExtensionPlugin.class));
        when(extensionLoader.loadExtensions()).thenReturn((List) extensionPlugins);

        createObjectUnderTest().applyExtensions();

        for (ExtensionPlugin extensionPlugin : extensionPlugins) {
            verify(extensionPlugin).apply(dataPrepperExtensionPoints);
        }
    }

    @Test
    void applyExtensions_with_empty_extensions_is_ok() {
        when(extensionLoader.loadExtensions()).thenReturn(Collections.emptyList());

        createObjectUnderTest().applyExtensions();
    }

    @Test
    void shutDownExtensions_invokes_extension_plugin_shutdown() {
        final ExtensionPlugin extensionPlugin = mock(ExtensionPlugin.class);
        when(extensionLoader.loadExtensions()).thenReturn((List) List.of(extensionPlugin));
        final ExtensionsApplier objectUnderTest = createObjectUnderTest();
        objectUnderTest.applyExtensions();
        objectUnderTest.shutdownExtensions();
        verify(extensionPlugin).shutdown();
    }

    @Test
    void errors_on_apply_are_handled_by_plugin_error_collector() {

        final ExtensionPlugin extensionPlugin = mock(ExtensionPlugin.class);
        final ExtensionPlugin otherExtensionPlugin = mock(ExtensionPlugin.class);
        final List<ExtensionPlugin> extensionPlugins = List.of(extensionPlugin, otherExtensionPlugin);
        when(extensionLoader.loadExtensions()).thenReturn((List) extensionPlugins);
        doThrow(RuntimeException.class).when(extensionPlugin).apply(dataPrepperExtensionPoints);
        doThrow(RuntimeException.class).when(otherExtensionPlugin).apply(dataPrepperExtensionPoints);

        final PluginError pluginError = mock(PluginError.class);
        when(pluginError.getComponentType()).thenReturn(PipelinesDataFlowModel.EXTENSION_PLUGIN_TYPE);
        final PluginError otherPluginError = mock(PluginError.class);
        when(otherPluginError.getComponentType()).thenReturn(PipelinesDataFlowModel.EXTENSION_PLUGIN_TYPE);
        when(pluginErrorCollector.getPluginErrors()).thenReturn(List.of(pluginError, otherPluginError));

        final ExtensionsApplier objectUnderTest = createObjectUnderTest();

        assertThrows(RuntimeException.class, objectUnderTest::applyExtensions);

        verify(pluginErrorCollector, times(2)).collectPluginError(any(PluginError.class));
        verify(pluginErrorsHandler).handleErrors(anyCollection());
    }
}