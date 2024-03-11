/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.reflections.Reflections;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

class ClasspathExtensionClassProviderTest {
    private Reflections reflections;

    @BeforeEach
    void setUp() {
        reflections = mock(Reflections.class);
    }

    private ClasspathExtensionClassProvider createObjectUnderTest() {
        return new ClasspathExtensionClassProvider(reflections);
    }

    @Test
    void loadExtensionPluginClasses_should_scan_for_plugins() {
        final ClasspathExtensionClassProvider objectUnderTest = createObjectUnderTest();

        then(reflections).shouldHaveNoInteractions();

        given(reflections.getSubTypesOf(ExtensionPlugin.class))
                .willReturn(Collections.emptySet());

        objectUnderTest.loadExtensionPluginClasses();

        then(reflections)
                .should()
                .getSubTypesOf(ExtensionPlugin.class);
    }

    @Test
    void loadExtensionPluginClasses_should_scan_for_plugins_only_once() {
        final ClasspathExtensionClassProvider objectUnderTest = createObjectUnderTest();

        given(reflections.getSubTypesOf(ExtensionPlugin.class))
                .willReturn(Collections.emptySet());

        for (int i = 0; i < 10; i++)
            objectUnderTest.loadExtensionPluginClasses();

        then(reflections)
                .should()
                .getSubTypesOf(ExtensionPlugin.class);
    }

    @Test
    void findPluginExtensionClass_should_return_empty_if_no_plugins_found() {
        given(reflections.getSubTypesOf(ExtensionPlugin.class))
                .willReturn(Collections.emptySet());

        final Collection<Class<? extends ExtensionPlugin>> extensionPluginClasses = createObjectUnderTest().loadExtensionPluginClasses();
        assertThat(extensionPluginClasses, notNullValue());
        assertThat(extensionPluginClasses.size(), equalTo(0));
    }

    @Test
    void findPluginExtensionClass_should_return_all_plugin_classes_found() {
        final Set<Class<? extends ExtensionPlugin>> classes = Set.of(
                mock(ExtensionPlugin.class).getClass()
        );
        given(reflections.getSubTypesOf(ExtensionPlugin.class))
                .willReturn(classes);

        final Collection<Class<? extends ExtensionPlugin>> extensionPluginClasses = createObjectUnderTest().loadExtensionPluginClasses();
        assertThat(extensionPluginClasses, notNullValue());
        assertThat(extensionPluginClasses.size(), equalTo(classes.size()));
        assertThat(extensionPluginClasses, equalTo(classes));
    }
}