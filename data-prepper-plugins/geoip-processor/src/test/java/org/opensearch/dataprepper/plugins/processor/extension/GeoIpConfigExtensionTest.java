/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
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

        final GeoIpServiceConfig geoIpServiceConfig = geoIpConfigSupplier.getGeoIpServiceConfig();
        assertThat(geoIpServiceConfig, notNullValue());

        final MaxMindConfig maxMindConfig = geoIpServiceConfig.getMaxMindConfig();
        assertThat(maxMindConfig, notNullValue());
        assertThat(maxMindConfig.getCacheSize(), equalTo(4096));
        assertThat(maxMindConfig.getDatabasePaths().size(), equalTo(0));
        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(7)));
    }

}