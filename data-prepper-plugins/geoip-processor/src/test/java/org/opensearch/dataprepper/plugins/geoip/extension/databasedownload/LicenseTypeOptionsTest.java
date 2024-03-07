/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class LicenseTypeOptionsTest {

    @Test
    void notNull_test() {
        assertNotNull(LicenseTypeOptions.ENTERPRISE);
    }

    @Test
    void fromOptionValue_test() {
        LicenseTypeOptions licenseTypeOptions = LicenseTypeOptions.fromOptionValue("enterprise");
        assertNotNull(licenseTypeOptions);
        assertThat(licenseTypeOptions.toString(), equalTo("ENTERPRISE"));
    }
}