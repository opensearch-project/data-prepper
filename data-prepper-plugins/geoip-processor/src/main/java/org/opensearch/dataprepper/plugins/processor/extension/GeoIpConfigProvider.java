/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

public class GeoIpConfigProvider implements ExtensionProvider<GeoIpConfigSupplier> {
    private final GeoIpConfigSupplier geoIpConfigSupplier;

    public GeoIpConfigProvider(final GeoIpConfigSupplier geoIpConfigSupplier) {
        this.geoIpConfigSupplier = geoIpConfigSupplier;
    }

    @Override
    public Optional<GeoIpConfigSupplier> provideInstance(Context context) {
        return Optional.of(this.geoIpConfigSupplier);
    }

    @Override
    public Class<GeoIpConfigSupplier> supportedClass() {
        return GeoIpConfigSupplier.class;
    }
}
