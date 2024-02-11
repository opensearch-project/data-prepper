/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIP2DatabaseReader;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoLite2DatabaseReader;
import org.opensearch.dataprepper.plugins.processor.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.processor.exception.NoValidDatabaseFoundException;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindConfig;
import org.opensearch.dataprepper.plugins.processor.utils.DatabaseSourceIdentification;
import org.opensearch.dataprepper.plugins.processor.utils.LicenseTypeCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSource.tempFolderPath;

public class GeoIPDatabaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(GeoIPDatabaseManager.class);
    public static final String FIRST_DATABASE_DIR = "first_database";
    public static final String SECOND_DATABASE_DIR = "second_database";
    private final MaxMindConfig maxMindConfig;
    private final LicenseTypeCheck licenseTypeCheck;
    private final DatabaseReaderBuilder databaseReaderBuilder;
    private final List<String> databasePaths;
    private final WriteLock writeLock;
    private final int cacheSize;
    private final GeoIPFileManager geoIPFileManager;
    private DBSourceOptions dbSourceOptions;
    private String currentDatabaseDir;
    private GeoIPDatabaseReader geoIPDatabaseReader;
    private boolean databaseDirToggle;
    private boolean downloadReady;

    public GeoIPDatabaseManager(final MaxMindConfig maxMindConfig,
                                final LicenseTypeCheck licenseTypeCheck,
                                final DatabaseReaderBuilder databaseReaderBuilder,
                                final GeoIPFileManager geoIPFileManager,
                                final ReentrantReadWriteLock.WriteLock writeLock
                                ) {
        this.maxMindConfig = maxMindConfig;
        this.licenseTypeCheck = licenseTypeCheck;
        this.databaseReaderBuilder = databaseReaderBuilder;
        this.geoIPFileManager = geoIPFileManager;
        this.databasePaths = maxMindConfig.getDatabasePaths();
        this.writeLock = writeLock;
        this.cacheSize = maxMindConfig.getCacheSize();
    }

    public void initiateDatabaseDownload() {
        dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(databasePaths);
        try {
            downloadDatabases();
        } catch (final Exception e) {
            throw new DownloadFailedException(e.getMessage());
        }
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
                try {
                    geoIPDatabaseReader = createReader();
                } catch (final NoValidDatabaseFoundException e) {
                    throw new NoValidDatabaseFoundException(e.getMessage());
                }
            }
        }
        downloadReady = false;
    }

    public void updateDatabaseReader() {
        try {
            downloadDatabases();
        } catch (final Exception e) {
            LOG.error("Database download failed, using previously loaded database. {}", e.getMessage());
            final File file = new File(currentDatabaseDir);
            geoIPFileManager.deleteDirectory(file);
            switchDirectory();
        }

        if (downloadReady) {
            downloadReady = false;
            try {
                switchDatabase();
            } catch (final Exception e) {
                LOG.error("Failed to update databases. Please make sure the database files exist at configured path amd are valid. " +
                        "Using previously loaded database. {}", e.getMessage());
                final File file = new File(currentDatabaseDir);
                geoIPFileManager.deleteDirectory(file);
                switchDirectory();
            }
        }
    }

    private void switchDatabase() {
        writeLock.lock();
        try {
            final GeoIPDatabaseReader newGeoipDatabaseReader = createReader();
            final GeoIPDatabaseReader oldGeoipDatabaseReader = geoIPDatabaseReader;
            geoIPDatabaseReader = newGeoipDatabaseReader;
            oldGeoipDatabaseReader.close();
        } finally {
            writeLock.unlock();
        }
    }

    private void downloadDatabases() throws Exception {
        DBSource dbSource;
        switchDirectory();

        switch (dbSourceOptions) {
            case CDN:
                dbSource = new CDNDownloadService(currentDatabaseDir);
                dbSource.initiateDownload(databasePaths);
                downloadReady =true;
                break;
            case URL:
                dbSource = new HttpDBDownloadService(currentDatabaseDir, geoIPFileManager);
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
    }

    private GeoIPDatabaseReader createReader() {
        final String finalPath = tempFolderPath + File.separator + currentDatabaseDir;
        final LicenseTypeOptions licenseType = licenseTypeCheck.isGeoLite2OrEnterpriseLicense(finalPath);
        if (licenseType == null) {
            throw new NoValidDatabaseFoundException("At least one valid database is required.");
        }
        GeoIPDatabaseReader newGeoIPDatabaseReader;
        if (licenseType.equals(LicenseTypeOptions.FREE)) {
            newGeoIPDatabaseReader = new GeoLite2DatabaseReader(databaseReaderBuilder, geoIPFileManager, finalPath, cacheSize);
        } else {
            newGeoIPDatabaseReader = new GeoIP2DatabaseReader(databaseReaderBuilder, geoIPFileManager, finalPath, cacheSize);
        }
        return newGeoIPDatabaseReader;
    }

    private void switchDirectory() {
        databaseDirToggle = !databaseDirToggle;
        if (databaseDirToggle) {
            currentDatabaseDir = FIRST_DATABASE_DIR;
        } else {
            currentDatabaseDir = SECOND_DATABASE_DIR;
        }
    }

    public GeoIPDatabaseReader getGeoIPDatabaseReader() {
        return geoIPDatabaseReader;
    }

    public void deleteDatabasesOnShutdown() {
        geoIPFileManager.deleteDirectory(new File(tempFolderPath + File.separator + FIRST_DATABASE_DIR));
        geoIPFileManager.deleteDirectory(new File(tempFolderPath + File.separator + SECOND_DATABASE_DIR));
    }

    public void deleteDirectory(final File file) {

        if (file.exists()) {
            for (final File subFile : file.listFiles()) {
                if (subFile.isDirectory()) {
                    deleteDirectory(subFile);
                }
                subFile.delete();
            }
            file.delete();
        }
    }
}
