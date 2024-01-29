/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LicenseTypeOptions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

@ExtendWith(MockitoExtension.class)
class LicenseTypeCheckTest {

    private static final String FOLDER_PATH_GEO_LITE2 = "./src/test/resources/mmdb-file/geo-lite2";
    private static final String FOLDER_PATH_GEO_ENTERPRISE = "./src/test/resources/mmdb-file/geo-enterprise";

    private LicenseTypeCheck createObjectUnderTest() {
        return new LicenseTypeCheck();
    }

    @Test
    void isGeoLite2OrEnterpriseLicenseTest_positive() {
        final LicenseTypeCheck objectUnderTest = createObjectUnderTest();
        LicenseTypeOptions licenseTypeOptionsFree = objectUnderTest.isGeoLite2OrEnterpriseLicense(FOLDER_PATH_GEO_LITE2);
        assertThat(licenseTypeOptionsFree, equalTo(LicenseTypeOptions.FREE));

        LicenseTypeOptions licenseTypeOptionsEnterprise = objectUnderTest.isGeoLite2OrEnterpriseLicense(FOLDER_PATH_GEO_ENTERPRISE);
        assertThat(licenseTypeOptionsEnterprise, equalTo(LicenseTypeOptions.ENTERPRISE));
    }

    @Test
    void isGeoLite2OrEnterpriseLicenseTest_negative() {
        final LicenseTypeCheck objectUnderTest = createObjectUnderTest();
        LicenseTypeOptions licenseTypeOptionsFree = objectUnderTest.isGeoLite2OrEnterpriseLicense(FOLDER_PATH_GEO_ENTERPRISE);
        assertThat(licenseTypeOptionsFree, not(equalTo(LicenseTypeOptions.FREE)));

        LicenseTypeOptions licenseTypeOptionsEnterprise = objectUnderTest.isGeoLite2OrEnterpriseLicense(FOLDER_PATH_GEO_LITE2);
        assertThat(licenseTypeOptionsEnterprise, not(equalTo(LicenseTypeOptions.ENTERPRISE)));
    }
}