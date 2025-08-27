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
        final Class<?>[] classes = {DefaultPluginFactory.class};

        ExtensionLoader.ExtensionPluginWithContext context1 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        ExtensionLoader.ExtensionPluginWithContext context2 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        Comparator<ExtensionLoader.ExtensionPluginWithContext> extensionsLoaderComparator = pluginCreatorContext.extensionsLoaderComparator();
        assertNotNull(extensionsLoaderComparator);
        when(context1.isConfigured()).thenReturn(true);
        when(context1.getDependentClasses()).thenReturn(classes);
        when(context1.getProvidedClasses()).thenReturn(new Class<?>[]{});

        when(context2.isConfigured()).thenReturn(true);
        when(context2.getProvidedClasses()).thenReturn(classes);
        when(context2.getDependentClasses()).thenReturn(new Class<?>[]{});
        assertThat(extensionsLoaderComparator.compare(context1, context2), equalTo(1));

        when(context1.isConfigured()).thenReturn(false);
        when(context1.getDependentClasses()).thenReturn(new Class<?>[]{});
        when(context1.getProvidedClasses()).thenReturn(classes);

        when(context2.isConfigured()).thenReturn(false);
        when(context2.getProvidedClasses()).thenReturn(new Class<?>[]{});
        when(context2.getDependentClasses()).thenReturn(classes);
        assertThat(extensionsLoaderComparator.compare(context1, context2), equalTo(-1));

        when(context1.getDependentClasses()).thenReturn(new Class<?>[]{});
        when(context1.getProvidedClasses()).thenReturn(new Class<?>[]{});
        when(context2.getProvidedClasses()).thenReturn(new Class<?>[]{});
        when(context2.getProvidedClasses()).thenReturn(new Class<?>[]{});

        when(context1.isConfigured()).thenReturn(false);
        when(context2.isConfigured()).thenReturn(true);

        assertThat(extensionsLoaderComparator.compare(context1, context2), greaterThan(0));
        when(context1.isConfigured()).thenReturn(true);
        when(context2.isConfigured()).thenReturn(false);
        assertThat(extensionsLoaderComparator.compare(context1, context2), lessThan(0));
    }

    @Test
    public void test_extensionsLoaderComparator_prioritizes_dependencies() {
        final Class<?>[] classes = {DefaultPluginFactory.class};

        ExtensionLoader.ExtensionPluginWithContext context1 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        ExtensionLoader.ExtensionPluginWithContext context2 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        Comparator<ExtensionLoader.ExtensionPluginWithContext> extensionsLoaderComparator = pluginCreatorContext.extensionsLoaderComparator();
        assertNotNull(extensionsLoaderComparator);

        when(context1.isConfigured()).thenReturn(false);
        when(context1.getDependentClasses()).thenReturn(new Class<?>[]{});
        when(context1.getProvidedClasses()).thenReturn(classes);

        when(context2.isConfigured()).thenReturn(true);
        when(context2.getProvidedClasses()).thenReturn(new Class<?>[]{});
        when(context2.getDependentClasses()).thenReturn(classes);

        assertThat(extensionsLoaderComparator.compare(context1, context2), equalTo(-1));
    }

    @Test
    public void test_extensionsLoaderComparator_falls_back_to_if_configured_when_there_are_no_dependencies() {
        ExtensionLoader.ExtensionPluginWithContext context1 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        ExtensionLoader.ExtensionPluginWithContext context2 = mock(ExtensionLoader.ExtensionPluginWithContext.class);
        Comparator<ExtensionLoader.ExtensionPluginWithContext> extensionsLoaderComparator = pluginCreatorContext.extensionsLoaderComparator();
        assertNotNull(extensionsLoaderComparator);

        when(context1.isConfigured()).thenReturn(false);
        when(context1.getDependentClasses()).thenReturn(new Class<?>[]{});
        when(context1.getProvidedClasses()).thenReturn(new Class<?>[]{});

        when(context2.isConfigured()).thenReturn(true);
        when(context2.getProvidedClasses()).thenReturn(new Class<?>[]{});
        when(context2.getDependentClasses()).thenReturn(new Class<?>[]{});

        assertThat(extensionsLoaderComparator.compare(context1, context2), equalTo(1));
    }
}
