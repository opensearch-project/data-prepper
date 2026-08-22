/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_ALTERNATE_NAME;
import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_DEPRECATED_NAME;

class LegacyPluginBundleActivatorTest {

    private static final String PLUGIN_PACKAGE = MultiNamedPlugin.class.getPackage().getName();

    private FelixPluginManager felixPluginManager;
    private LegacyPluginBundleActivator activator;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        activator = new LegacyPluginBundleActivator();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void start_with_no_plugin_classes_header_does_nothing() throws Exception {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        // System bundle has no DataPrepper-Plugin-Classes header
        activator.start(ctx);
        // Should complete without error
    }

    @Test
    void stop_with_no_registrations_does_nothing() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        activator.stop(ctx);
        // Should complete without error
    }

    @Test
    void start_then_stop_lifecycle() throws Exception {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        activator.start(ctx);
        activator.stop(ctx);
        // Should complete without error
    }

    @Test
    void collectPluginNames_with_default_deprecated_and_alternate_names_returns_only_the_primary_name() {
        final DataPrepperPlugin annotation = pluginAnnotation(
                "my-plugin", DEFAULT_DEPRECATED_NAME, new String[] {});

        assertThat(LegacyPluginBundleActivator.collectPluginNames(annotation),
                arrayContaining("my-plugin"));
    }

    @Test
    void collectPluginNames_with_deprecated_name_returns_primary_name_then_deprecated_name() {
        final DataPrepperPlugin annotation = pluginAnnotation(
                "my-plugin", "my_plugin", new String[] {});

        assertThat(LegacyPluginBundleActivator.collectPluginNames(annotation),
                arrayContaining("my-plugin", "my_plugin"));
    }

    @Test
    void collectPluginNames_with_alternate_names_returns_primary_name_then_alternate_names() {
        final DataPrepperPlugin annotation = pluginAnnotation(
                "my-plugin", DEFAULT_DEPRECATED_NAME, new String[] {"alternate-one", "alternate-two"});

        assertThat(LegacyPluginBundleActivator.collectPluginNames(annotation),
                arrayContaining("my-plugin", "alternate-one", "alternate-two"));
    }

    @Test
    void collectPluginNames_with_all_name_kinds_returns_name_then_deprecated_name_then_alternate_names() {
        final DataPrepperPlugin annotation = pluginAnnotation(
                "my-plugin", "my_plugin", new String[] {"alternate-one", "alternate-two"});

        assertThat(LegacyPluginBundleActivator.collectPluginNames(annotation),
                arrayContaining("my-plugin", "my_plugin", "alternate-one", "alternate-two"));
    }

    @Test
    void collectPluginNames_with_default_alternate_name_entry_omits_that_entry() {
        final DataPrepperPlugin annotation = pluginAnnotation(
                "my-plugin", DEFAULT_DEPRECATED_NAME, new String[] {DEFAULT_ALTERNATE_NAME, "alternate-one"});

        assertThat(LegacyPluginBundleActivator.collectPluginNames(annotation),
                arrayContaining("my-plugin", "alternate-one"));
    }

    @Test
    void start_publishes_every_plugin_name_in_the_multi_valued_name_property() throws Exception {
        final Bundle bundle = mock(Bundle.class);
        final BundleContext context = mock(BundleContext.class);
        final Dictionary<String, String> headers = new Hashtable<>();
        headers.put("DataPrepper-Plugin-Classes", PLUGIN_PACKAGE);
        when(context.getBundle()).thenReturn(bundle);
        when(bundle.getHeaders()).thenReturn(headers);
        when(bundle.findEntries("/" + PLUGIN_PACKAGE.replace('.', '/'), "*.class", true))
                .thenReturn(Collections.enumeration(
                        Collections.singletonList(classFileUrl(MultiNamedPlugin.class))));
        doReturn(MultiNamedPlugin.class).when(bundle).loadClass(MultiNamedPlugin.class.getName());

        activator.start(context);

        final ArgumentCaptor<Dictionary<String, Object>> properties = ArgumentCaptor.forClass(Dictionary.class);
        verify(context).registerService(eq(Class.class.getName()), eq(MultiNamedPlugin.class), properties.capture());

        // Every name the plugin answers to is published as one multi-valued property. Publishing only
        // the primary name, as a plain string, would still resolve that name and silently lose the rest.
        final Object publishedNames = properties.getValue().get("dataprepper.plugin.name");
        assertThat("Plugin names must be published as a multi-valued property",
                publishedNames, is(instanceOf(String[].class)));
        assertThat((String[]) publishedNames,
                arrayContaining("multi-named-plugin", "multi_named_plugin", "alternate-multi-named-plugin"));
        assertThat(properties.getValue().get("dataprepper.plugin.type"),
                is(MultiNamedPluginType.class.getName()));
        assertThat(properties.getValue().get("dataprepper.plugin.class"),
                is(MultiNamedPlugin.class.getName()));
    }

    private static DataPrepperPlugin pluginAnnotation(final String name,
                                                     final String deprecatedName,
                                                     final String[] alternateNames) {
        final DataPrepperPlugin annotation = mock(DataPrepperPlugin.class);
        when(annotation.name()).thenReturn(name);
        when(annotation.deprecatedName()).thenReturn(deprecatedName);
        when(annotation.alternateNames()).thenReturn(alternateNames);
        return annotation;
    }

    /**
     * Builds the bundle entry URL the activator would find for a class, so that the class name the
     * activator derives from the entry path is the class this test actually stubs.
     *
     * @param pluginClass the class to build an entry URL for
     * @return the {@code file:} URL of that class's bundle entry
     */
    private static URL classFileUrl(final Class<?> pluginClass) throws MalformedURLException {
        return new URL("file:/" + pluginClass.getName().replace('.', '/') + ".class");
    }

    interface MultiNamedPluginType {
    }

    @DataPrepperPlugin(name = "multi-named-plugin", deprecatedName = "multi_named_plugin",
            alternateNames = {"alternate-multi-named-plugin"}, pluginType = MultiNamedPluginType.class)
    static class MultiNamedPlugin implements MultiNamedPluginType {
    }
}
