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
    private static final String GEOIP2_DATABASE = "geoip2";
    private static final String GEOLITE2_DATABASE = "geolite2";
    private static final String MMDB = "mmdb";


    public LicenseTypeCheck() {
    }

    /**
     * Get the license type based on the maxmind mmdb file name
     *
     * @param databasePath databasePath
     * @return license type options
     */
    public LicenseTypeOptions isGeoLite2OrEnterpriseLicense(final String databasePath) {
        LicenseTypeOptions licenseTypeOptions = null;
        final File directory = new File(databasePath);
        if (directory.isDirectory()) {
            // list all files present in the directory
            final File[] files = directory.listFiles();

            for (final File file : files) {
                // convert the file name into string
                final String fileName = file.toString();

                int index = fileName.lastIndexOf('.');
                if (index > 0) {
                    String extension = fileName.substring(index + 1);
                    Path onlyFileName = Paths.get(fileName).getFileName();

                    if ((extension.equals(MMDB)) && (onlyFileName.toString().toLowerCase().contains(GEOIP2_DATABASE))) {
                        licenseTypeOptions = LicenseTypeOptions.ENTERPRISE;
                        break;
                    } else if ((extension.equals(MMDB)) && (onlyFileName.toString().toLowerCase().contains(GEOLITE2_DATABASE))) {
                        licenseTypeOptions = LicenseTypeOptions.FREE;
                    }
                }
            }
        }
        return licenseTypeOptions;
    }
}
