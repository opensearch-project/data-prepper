/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.MatcherAssert.assertThat;
import java.util.Comparator;

public class PluginCreatorContextTest {
    PluginCreatorContext pluginCreatorContext;

    @BeforeEach
    void setUp() {
        pluginCreatorContext = new PluginCreatorContext();
    }
    
    @Test
    public void test_observablePluginCreator() {
        PluginCreator pluginCreator = pluginCreatorContext.observablePluginCreator();
        assertNotNull(pluginCreator);
    }

    @Test
    public void test_pluginCreator() {
        PluginConfigurationObservableRegister pluginConfigurationObservableRegister = mock(PluginConfigurationObservableRegister.class);
        PluginCreator pluginCreator = pluginCreatorContext.pluginCreator(pluginConfigurationObservableRegister);
        assertNotNull(pluginCreator);
    }

    @Test
    public void test_extensionsLoaderComparator() {
        ExtensionLoader.ExtensionPluginWithContext context1 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        ExtensionLoader.ExtensionPluginWithContext context2 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        Comparator<ExtensionLoader.ExtensionPluginWithContext> extensionsLoaderComparator = pluginCreatorContext.extensionsLoaderComparator();
        assertNotNull(extensionsLoaderComparator);
        when(context1.isConfigured()).thenReturn(true);
        when(context2.isConfigured()).thenReturn(true);
        assertThat(extensionsLoaderComparator.compare(context1, context2), equalTo(0));
        when(context1.isConfigured()).thenReturn(false);
        when(context2.isConfigured()).thenReturn(false);
        assertThat(extensionsLoaderComparator.compare(context1, context2), equalTo(0));
        when(context1.isConfigured()).thenReturn(false);
        when(context2.isConfigured()).thenReturn(true);
        assertThat(extensionsLoaderComparator.compare(context1, context2), greaterThan(0));
        when(context1.isConfigured()).thenReturn(true);
        when(context2.isConfigured()).thenReturn(false);
        assertThat(extensionsLoaderComparator.compare(context1, context2), lessThan(0));
    }
}
