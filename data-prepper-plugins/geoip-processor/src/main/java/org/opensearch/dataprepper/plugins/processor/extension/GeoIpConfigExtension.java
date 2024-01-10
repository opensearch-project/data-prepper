/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

@DataPrepperExtensionPlugin(modelType = GeoIpServiceConfig.class, rootKeyJsonPath = "/geoip_service")
public class GeoIpConfigExtension implements ExtensionPlugin {
    private final DefaultGeoIpConfigSupplier defaultGeoIpConfigSupplier;

    @DataPrepperPluginConstructor
    public GeoIpConfigExtension(final GeoIpServiceConfig geoIpServiceConfig) {
        this.defaultGeoIpConfigSupplier = new DefaultGeoIpConfigSupplier(geoIpServiceConfig != null ? geoIpServiceConfig : new GeoIpServiceConfig());
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new GeoIpConfigProvider(this.defaultGeoIpConfigSupplier));
    }
}
