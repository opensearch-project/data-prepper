/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.GeoIPDatabaseManager;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultGeoIpConfigSupplier implements GeoIpConfigSupplier {
    private final GeoIpServiceConfig geoIpServiceConfig;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final GeoIPDatabaseManager geoIPDatabaseManager;

    public DefaultGeoIpConfigSupplier(final GeoIpServiceConfig geoIpServiceConfig,
                                      final GeoIPDatabaseManager geoIPDatabaseManager,
                                      final ReentrantReadWriteLock.ReadLock readLock
                                      ) {
        this.geoIpServiceConfig = geoIpServiceConfig;
        this.geoIPDatabaseManager = geoIPDatabaseManager;
        this.readLock = readLock;
    }

    @Override
    public GeoIPProcessorService getGeoIPProcessorService() {
        if (geoIpServiceConfig != null)
            return new GeoIPProcessorService(geoIpServiceConfig, geoIPDatabaseManager, readLock);
        else
            return null;
    }
}
