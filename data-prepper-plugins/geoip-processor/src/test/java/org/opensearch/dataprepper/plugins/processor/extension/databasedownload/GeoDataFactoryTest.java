/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoData;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoIP2Data;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoLite2Data;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindConfig;
import org.opensearch.dataprepper.plugins.processor.utils.LicenseTypeCheck;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoDataFactoryTest {

    @Mock
    private MaxMindConfig maxMindConfig;

    @Mock
    private LicenseTypeCheck licenseTypeCheck;

    @Test
    void testCreateWithFreeLicense() {
        when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(anyString())).thenReturn(LicenseTypeOptions.FREE);
        final GeoDataFactory geoDataFactory = new GeoDataFactory(maxMindConfig, licenseTypeCheck);
        final String databasePath = "testPath";

        final GetGeoData getGeoData = geoDataFactory.create(databasePath);
        assertInstanceOf(GetGeoLite2Data.class, getGeoData);
    }

    @Test
    void testCreateWithEnterpriseLicense() {
        when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(anyString())).thenReturn(LicenseTypeOptions.ENTERPRISE);
        final GeoDataFactory geoDataFactory = new GeoDataFactory(maxMindConfig, licenseTypeCheck);
        final String databasePath = "testPath";

        final GetGeoData getGeoData = geoDataFactory.create(databasePath);
        assertInstanceOf(GetGeoIP2Data.class, getGeoData);
    }

}