/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.database;

import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoData;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoIP2Data;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoLite2Data;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindConfig;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSource;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSourceOptions;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.HttpDBDownloadService;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LicenseTypeOptions;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LocalDBDownloadService;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.S3DBService;
import org.opensearch.dataprepper.plugins.processor.utils.DbSourceIdentification;
import org.opensearch.dataprepper.plugins.processor.utils.LicenseTypeCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSource.tempFolderPath;

public class GeoIPDatabaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(GeoIPDatabaseManager.class);
    public static final String FIRST_DATABASE_DIR = "first_database";
    public static final String SECOND_DATABASE_DIR = "second_database";
    private final DBSourceOptions dbSourceOptions;
    private final List<String> databasePaths;
    private final MaxMindConfig maxMindConfig;
    private final LicenseTypeCheck licenseTypeCheck;
    private String currentDatabaseDir;
    private GetGeoData geoData;
    private boolean databaseDirToggle;
    private boolean downloadReady;

    public GeoIPDatabaseManager(final MaxMindConfig maxMindConfig) {
        this.maxMindConfig = maxMindConfig;
        this.licenseTypeCheck = new LicenseTypeCheck();
        this.databasePaths = maxMindConfig.getDatabasePaths();
        this.dbSourceOptions = DbSourceIdentification.getDatabasePathType(databasePaths);

        downloadDatabases();
        synchronized (this) {
            try {
                while (!downloadReady) {
                    wait();
                }
            } catch (final InterruptedException ex) {
                LOG.info("Thread interrupted while waiting for download to complete: {0}",  ex);
                Thread.currentThread().interrupt();
            }
            if (downloadReady) {
                createReader();
            }
        }
        downloadReady = false;
    }

    public void updateDatabaseReader() {
        downloadDatabases();

        if (!downloadReady) {
            switchDirectory();
        } else {
            downloadReady = false;
            createReader();
        }
    }

    private void downloadDatabases() {
        DBSource dbSource;
        switchDirectory();

        try {
            switch (dbSourceOptions) {
                case URL:
                    dbSource = new HttpDBDownloadService(currentDatabaseDir);
                    dbSource.initiateDownload(databasePaths);
                    downloadReady = true;
                    break;
                case S3:
                    dbSource = new S3DBService(maxMindConfig.getAwsAuthenticationOptionsConfig(), currentDatabaseDir);
                    dbSource.initiateDownload(databasePaths);
                    downloadReady = true;
                    break;
                case PATH:
                    dbSource = new LocalDBDownloadService(currentDatabaseDir);
                    dbSource.initiateDownload(databasePaths);
                    downloadReady = true;
                    break;
            }
        } catch (final Exception ex) {
            LOG.error("Database download failed, using previously loaded database.", ex);
        }
    }

    private void createReader() {
        final String finalPath = tempFolderPath + File.separator + currentDatabaseDir;
        final LicenseTypeOptions licenseType = licenseTypeCheck.isGeoLite2OrEnterpriseLicense(finalPath);
        if (licenseType.equals(LicenseTypeOptions.FREE)) {
            geoData = new GetGeoLite2Data(finalPath, maxMindConfig.getCacheSize());
        } else {
            geoData = new GetGeoIP2Data(finalPath, maxMindConfig.getCacheSize());
        }
    }

    private void switchDirectory() {
        databaseDirToggle = !databaseDirToggle;
        if (databaseDirToggle) {
            currentDatabaseDir = FIRST_DATABASE_DIR;
        } else {
            currentDatabaseDir = SECOND_DATABASE_DIR;
        }
    }

    public GetGeoData getGeoData() {
        return geoData;
    }
}
