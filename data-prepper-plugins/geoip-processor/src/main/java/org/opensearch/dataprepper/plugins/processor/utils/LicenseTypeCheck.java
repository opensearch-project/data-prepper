/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LicenseTypeOptions;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;


/**cls
 * 
 * Implementation of class logic to check maxmind database id free or enterprise version
 */
public class LicenseTypeCheck {

    protected static final String[] DEFAULT_DATABASE_FILENAMES = new String[] { "GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb" };
    protected static final List<String> geoLite2Database = Arrays.asList(DEFAULT_DATABASE_FILENAMES);
    private static final String geoIP2EnterpriseDB = "GeoIP2-Enterprise.mmdb";


    private LicenseTypeCheck() {

    }

    /**
     * Get the license type based on the maxmind mmdb file name
     * @param databasePath databasePath
     * @return license type options
     */
    public static LicenseTypeOptions isGeoLite2OrEnterpriseLicense(String databasePath) {
        LicenseTypeOptions licenseTypeOptions = LicenseTypeOptions.ENTERPRISE;
        File directory = new File(databasePath);
        // list all files present in the directory
        File[] files = directory.listFiles();

        for(File file : files) {
            // convert the file name into string
            String fileName = file.toString();

            int index = fileName.lastIndexOf('.');
            if(index > 0) {
                String extension = fileName.substring(index + 1);
                Path onlyFileName = Paths.get(fileName).getFileName();

                if((extension.equals("mmdb")) && (geoIP2EnterpriseDB.equals(onlyFileName.toString()))) {
                    licenseTypeOptions = LicenseTypeOptions.ENTERPRISE;
                    break;
                }
                else if((extension.equals("mmdb")) && (geoLite2Database.contains(onlyFileName.toString())))
                {
                    licenseTypeOptions = LicenseTypeOptions.FREE;
                }
            }
        }
        return licenseTypeOptions;
    }
}

