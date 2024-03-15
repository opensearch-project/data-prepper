/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.api;

import org.opensearch.dataprepper.plugins.geoip.extension.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.geoip.extension.GeoIpConfigExtension;

import java.util.Optional;

/**
 * Interface for supplying {@link GeoIPProcessorService} to {@link GeoIpConfigExtension}
 *
 * @since 2.7
 */
public interface GeoIpConfigSupplier {
    /**
     * Returns the {@link GeoIPProcessorService}
     *
     * @since 2.7
     */
    Optional<GeoIPProcessorService> getGeoIPProcessorService();
}
