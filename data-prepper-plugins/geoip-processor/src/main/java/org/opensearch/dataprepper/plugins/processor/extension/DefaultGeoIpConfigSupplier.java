/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

public class DefaultGeoIpConfigSupplier implements GeoIpConfigSupplier {
    private final GeoIpServiceConfig geoIpServiceConfig;

    public DefaultGeoIpConfigSupplier(final GeoIpServiceConfig geoIpServiceConfig) {
        this.geoIpServiceConfig = geoIpServiceConfig;
    }

    @Override
    public GeoIpServiceConfig getGeoIpServiceConfig() {
        return this.geoIpServiceConfig;
    }
}
