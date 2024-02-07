/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LicenseTypeOptions;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * 
 * Implementation of class logic to check maxmind database id free or enterprise version
 */
public class LicenseTypeCheck {
    private static final String geoIP2Database = "geoip2";
    private static final String geoLite2Database = "geolite2";


    public LicenseTypeCheck() {

    }

    /**
     * Get the license type based on the maxmind mmdb file name
     *
     * @param databasePath databasePath
     * @return license type options
     */
    public LicenseTypeOptions isGeoLite2OrEnterpriseLicense(final String databasePath) {
        LicenseTypeOptions licenseTypeOptions = LicenseTypeOptions.FREE;
        File directory = new File(databasePath);
        // list all files present in the directory
        File[] files = directory.listFiles();

        for(File file : files) {
            // convert the file name into string
            final String fileName = file.toString();

            int index = fileName.lastIndexOf('.');
            if (index > 0) {
                String extension = fileName.substring(index + 1);
                Path onlyFileName = Paths.get(fileName).getFileName();

                if((extension.equals("mmdb")) && (onlyFileName.toString().toLowerCase().contains(geoIP2Database))) {
                    licenseTypeOptions = LicenseTypeOptions.ENTERPRISE;
                    break;
                } else if((extension.equals("mmdb")) && (onlyFileName.toString().toLowerCase().contains(geoLite2Database))) {
                    licenseTypeOptions = LicenseTypeOptions.FREE;
                }
            }
        }
        return licenseTypeOptions;
    }
}

