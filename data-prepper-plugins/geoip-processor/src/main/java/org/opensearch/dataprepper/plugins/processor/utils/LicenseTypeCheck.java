/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.opensearch.dataprepper.plugins.processor.databasedownload.LicenseTypeOptions;


/**cls
 * 
 * Implementation of class logic to check maxmind database id free or enterprise version
 */
public class LicenseTypeCheck {

    private LicenseTypeCheck() {
        //TODO
    }

    /**
     * Get the license type based on the maxmind mmdb file name
     * @param databasePath databasePath
     * @return license type options
     */
    public static LicenseTypeOptions isGeoLite2OrEnterpriseLicense(String databasePath) {
       //TODO : Get the license type based on the maxmind mmdb file name
        return null;
    }
}

