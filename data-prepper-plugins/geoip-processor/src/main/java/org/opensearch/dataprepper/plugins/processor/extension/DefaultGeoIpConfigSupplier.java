/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorService;

public class DefaultGeoIpConfigSupplier implements GeoIpConfigSupplier {
    private final GeoIpServiceConfig geoIpServiceConfig;

    public DefaultGeoIpConfigSupplier(final GeoIpServiceConfig geoIpServiceConfig) {
        this.geoIpServiceConfig = geoIpServiceConfig;
    }

    @Override
    public GeoIPProcessorService getGeoIPProcessorService() {
        //TODO: use GeoIpServiceConfig and return GeoIPProcessorService
        return null;
    }
}
