/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoIpConfigExtensionTest {
    @Mock
    private ExtensionPoints extensionPoints;
    @Mock
    private GeoIpServiceConfig geoIpServiceConfig;
    @Mock
    private MaxMindConfig maxMindConfig;


    @BeforeEach
    void setUp() {
        when(geoIpServiceConfig.getMaxMindConfig()).thenReturn(maxMindConfig);
    }

    private GeoIpConfigExtension createObjectUnderTest() {
        return new GeoIpConfigExtension(geoIpServiceConfig);
    }

    @Test
    void apply_should_addExtensionProvider() {
        createObjectUnderTest().apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);

        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();

        assertThat(actualExtensionProvider, instanceOf(GeoIpConfigProvider.class));
    }

    @Test
    void extension_should_create_supplier_with_provided_config() {
        try (final MockedConstruction<GeoIpServiceConfig> mockedConstruction =
                     mockConstruction(GeoIpServiceConfig.class)) {
            final GeoIpConfigExtension geoIpConfigExtension = new GeoIpConfigExtension(geoIpServiceConfig);

            assertThat(mockedConstruction.constructed().size(), equalTo(0));
            assertThat(geoIpConfigExtension, instanceOf(GeoIpConfigExtension.class));
        }
    }
}
