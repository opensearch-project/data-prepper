/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.extension.databasedownload.LicenseTypeOptions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

@ExtendWith(MockitoExtension.class)
class LicenseTypeCheckTest {

    private static final String FOLDER_PATH_GEO_LITE2 = "./build/resources/test/mmdb-files/geo-lite2";
    private static final String FOLDER_PATH_GEO_ENTERPRISE = "./build/resources/test/mmdb-files/geo-enterprise";

    private LicenseTypeCheck createObjectUnderTest() {
        return new LicenseTypeCheck();
    }

    @Test
    void test_isGeoLite2OrEnterpriseLicenseTest_should_return_free_when_geolite2_databases_are_used() {
        final LicenseTypeCheck objectUnderTest = createObjectUnderTest();
        LicenseTypeOptions licenseTypeOptionsFree = objectUnderTest.isGeoLite2OrEnterpriseLicense(FOLDER_PATH_GEO_LITE2);
        assertThat(licenseTypeOptionsFree, equalTo(LicenseTypeOptions.FREE));
    }

    @Test
    void test_isGeoLite2OrEnterpriseLicenseTest_should_return_enterprise_when_geoip2_databases_are_used() {
        final LicenseTypeCheck objectUnderTest = createObjectUnderTest();
        LicenseTypeOptions licenseTypeOptionsFree = objectUnderTest.isGeoLite2OrEnterpriseLicense(FOLDER_PATH_GEO_ENTERPRISE);
        assertThat(licenseTypeOptionsFree, not(equalTo(LicenseTypeOptions.ENTERPRISE)));
    }
}