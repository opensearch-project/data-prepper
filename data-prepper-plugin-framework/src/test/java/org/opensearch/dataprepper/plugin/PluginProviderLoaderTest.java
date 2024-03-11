/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

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
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class PluginProviderLoaderTest {
    private ServiceLoader<PluginProvider> serviceLoader;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        serviceLoader = mock(ServiceLoader.class);
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
}