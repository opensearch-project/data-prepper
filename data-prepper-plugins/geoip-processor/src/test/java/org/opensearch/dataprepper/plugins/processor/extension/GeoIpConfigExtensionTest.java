/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorService;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class GeoIpConfigExtensionTest {
    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private GeoIpServiceConfig geoIpServiceConfig;

    @Mock
    private ExtensionProvider.Context context;

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
    void apply_should_addExtensionProvider_and_supplier_should_use_default_config_if_not_configured() {
        final GeoIpConfigExtension geoIpConfigExtension = new GeoIpConfigExtension(null);

        geoIpConfigExtension.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);

        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider actualExtensionProvider = extensionProviderArgumentCaptor.getValue();

        assertThat(actualExtensionProvider, instanceOf(GeoIpConfigProvider.class));

        final GeoIpConfigSupplier geoIpConfigSupplier = (GeoIpConfigSupplier) actualExtensionProvider.provideInstance(context).get();

        final GeoIPProcessorService geoIPProcessorService = geoIpConfigSupplier.getGeoIPProcessorService();

        //TODO: Update assertions after updating the supplier with GeoIPProcessorService
        assertThat(geoIPProcessorService, nullValue());

    }

}