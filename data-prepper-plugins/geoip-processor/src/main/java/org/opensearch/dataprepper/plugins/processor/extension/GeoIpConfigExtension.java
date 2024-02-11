/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.GeoIPFileManager;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DatabaseReaderBuilder;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.GeoIPDatabaseManager;
import org.opensearch.dataprepper.plugins.processor.utils.LicenseTypeCheck;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@DataPrepperExtensionPlugin(modelType = GeoIpServiceConfig.class, rootKeyJsonPath = "/geoip_service", allowInPipelineConfigurations = true)
public class GeoIpConfigExtension implements ExtensionPlugin {
    private final DefaultGeoIpConfigSupplier defaultGeoIpConfigSupplier;

    @DataPrepperPluginConstructor
    public GeoIpConfigExtension(final GeoIpServiceConfig geoIpServiceConfig) {
            final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
            GeoIPDatabaseManager geoIPDatabaseManager = null;
            if (geoIpServiceConfig != null) {
                geoIPDatabaseManager = new GeoIPDatabaseManager(
                        geoIpServiceConfig.getMaxMindConfig(),
                        new LicenseTypeCheck(),
                        new DatabaseReaderBuilder(),
                        new GeoIPFileManager(),
                        reentrantReadWriteLock.writeLock()
                );
            }
            this.defaultGeoIpConfigSupplier = new DefaultGeoIpConfigSupplier(geoIpServiceConfig, geoIPDatabaseManager, reentrantReadWriteLock.readLock());
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
            extensionPoints.addExtensionProvider(new GeoIpConfigProvider(this.defaultGeoIpConfigSupplier));
    }
}
