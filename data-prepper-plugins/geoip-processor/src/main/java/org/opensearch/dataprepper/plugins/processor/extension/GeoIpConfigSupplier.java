/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

public interface GeoIpConfigSupplier {
    GeoIpServiceConfig getGeoIpServiceConfig();
}
