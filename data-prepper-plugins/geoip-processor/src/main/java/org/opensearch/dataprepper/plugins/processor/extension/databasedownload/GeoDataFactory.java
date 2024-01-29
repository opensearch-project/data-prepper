/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoData;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoIP2Data;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoLite2Data;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindConfig;
import org.opensearch.dataprepper.plugins.processor.utils.LicenseTypeCheck;

import java.io.File;

import static org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSource.tempFolderPath;

public class GeoDataFactory {
    private final MaxMindConfig maxMindConfig;
    private final LicenseTypeCheck licenseTypeCheck;

    public GeoDataFactory(final MaxMindConfig maxMindConfig, final LicenseTypeCheck licenseTypeCheck) {
        this.maxMindConfig = maxMindConfig;
        this.licenseTypeCheck = licenseTypeCheck;
    }

    /**
     * Creates GetGeoData class based on LicenseTypeOptions
     */
    public GetGeoData create(final String databasePath) {
        final String finalPath = tempFolderPath + File.separator + databasePath;
        final LicenseTypeOptions licenseType = licenseTypeCheck.isGeoLite2OrEnterpriseLicense(finalPath);
        if (licenseType.equals(LicenseTypeOptions.FREE)) {
            return new GetGeoLite2Data(finalPath, maxMindConfig.getCacheSize());
        } else {
            return new GetGeoIP2Data(finalPath, maxMindConfig.getCacheSize());
        }
    }
}
