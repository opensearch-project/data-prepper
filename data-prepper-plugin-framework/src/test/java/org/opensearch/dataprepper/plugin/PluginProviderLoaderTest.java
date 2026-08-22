/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class PluginProviderLoaderTest {
    private static final String PLUGIN_FRAMEWORK_PROPERTY = "data-prepper.plugin.framework";

    private ServiceLoader<PluginProvider> serviceLoader;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        serviceLoader = mock(ServiceLoader.class);
    }

    @AfterEach
    void tearDown() {
        System.clearProperty(PLUGIN_FRAMEWORK_PROPERTY);
    }

    @SuppressWarnings("rawtypes")
    PluginProviderLoader createObjectUnderTest() {
        try (final MockedStatic<ServiceLoader> serviceLoaderStatic = mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(PluginProvider.class))
                    .thenReturn(serviceLoader);
            return new PluginProviderLoader();
        }
    }

    @Test
    void getPluginProviders_returns_empty_if_no_plugin_providers_loaded() {
        given(serviceLoader.spliterator())
                .willReturn(Collections.<PluginProvider>emptyList().spliterator());

        final Collection<PluginProvider> actualPluginProviders = createObjectUnderTest().getPluginProviders();

        assertThat(actualPluginProviders, notNullValue());
        assertThat(actualPluginProviders.size(), equalTo(0));
    }

    @Test
    void getPluginProviders_returns_a_collection_of_all_PluginProvider_instances() {
        final PluginProvider providerA = mock(PluginProvider.class);
        final PluginProvider providerB = mock(PluginProvider.class);
        final List<PluginProvider> originalPluginProviders = Arrays.asList(providerA, providerB);

        given(serviceLoader.spliterator()).willReturn(originalPluginProviders.spliterator());

        final Collection<PluginProvider> actualPluginProviders = createObjectUnderTest().getPluginProviders();

        assertThat(actualPluginProviders, not(sameInstance(originalPluginProviders)));
        assertThat(actualPluginProviders, equalTo(originalPluginProviders));
    }

    @Test
    void getPluginProviders_in_osgi_mode_returns_registered_providers_before_classpath_providers() {
        // The framework mode is cached at construction, so it must be set before the loader is created.
        System.setProperty(PLUGIN_FRAMEWORK_PROPERTY, "osgi");

        final PluginProvider classpathProvider = mock(PluginProvider.class);
        given(serviceLoader.spliterator())
                .willReturn(Collections.singletonList(classpathProvider).spliterator());
        final PluginProviderLoader objectUnderTest = createObjectUnderTest();

        final PluginProvider osgiProvider = mock(PluginProvider.class);
        objectUnderTest.registerProvider(osgiProvider);

        final List<PluginProvider> actualPluginProviders = new ArrayList<>(objectUnderTest.getPluginProviders());

        assertThat(actualPluginProviders, contains(osgiProvider, classpathProvider));
    }

    @Test
    void getPluginProviders_caches_mode_at_construction_and_ignores_later_property_change() {
        // Construct with legacy mode (default)
        given(serviceLoader.spliterator())
                .willReturn(Collections.<PluginProvider>emptyList().spliterator());
        final PluginProviderLoader loader = createObjectUnderTest();

        // Register an additional provider
        final PluginProvider osgiProvider = mock(PluginProvider.class);
        loader.registerProvider(osgiProvider);

        // Even if we change the system property now, the mode was cached at construction
        System.setProperty(PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        try {
            given(serviceLoader.spliterator())
                    .willReturn(Collections.<PluginProvider>emptyList().spliterator());
            final Collection<PluginProvider> providers = loader.getPluginProviders();
            // Should use legacy behavior since the mode was cached as "legacy" at construction
            assertThat(providers.size(), equalTo(0));
        } finally {
            System.clearProperty(PLUGIN_FRAMEWORK_PROPERTY);
        }
    }
}