/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.AutoCountingDatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.GeoIP2DatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.GeoLite2DatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.geoip.exception.NoValidDatabaseFoundException;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindConfig;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;
import org.opensearch.dataprepper.plugins.geoip.utils.DatabaseSourceIdentification;
import org.opensearch.dataprepper.plugins.geoip.utils.LicenseTypeCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class GeoIPDatabaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(GeoIPDatabaseManager.class);
    public static final String BLUE_DATABASE_DIR = "blue_database";
    public static final String GREEN_DATABASE_DIR = "green_database";
    private static final long INITIAL_DELAY = Duration.ofMinutes(1).toMillis();
    private static final long MAXIMUM_DELAY = Duration.ofHours(1).toMillis();
    private static final double JITTER_RATE = 0.15;
    private final MaxMindConfig maxMindConfig;
    private final LicenseTypeCheck licenseTypeCheck;
    private final DatabaseReaderBuilder databaseReaderBuilder;
    private final MaxMindDatabaseConfig maxMindDatabaseConfig;
    private final WriteLock writeLock;
    private final int cacheSize;
    private final GeoIPFileManager geoIPFileManager;
    private final AtomicInteger failedAttemptCount;
    private final Backoff backoff;
    private final DBSourceOptions dbSourceOptions;
    private String currentDatabaseDir;
    private GeoIPDatabaseReader geoIPDatabaseReader;
    private boolean databaseDirToggle;
    private Instant nextUpdateAt;

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
        this.maxMindDatabaseConfig = maxMindConfig.getMaxMindDatabaseConfig();
        this.writeLock = writeLock;
        this.cacheSize = maxMindConfig.getCacheSize();
        this.failedAttemptCount = new AtomicInteger(0);
        this.backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY)
                .withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        this.dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(new ArrayList<>(
                maxMindDatabaseConfig.getDatabasePaths().values()));

    }

    public void initiateDatabaseDownload() {
        try {
            downloadDatabases();
            geoIPDatabaseReader = createReader();
            nextUpdateAt = Instant.now().plus(maxMindConfig.getDatabaseRefreshInterval());
            failedAttemptCount.set(0);
        } catch (final Exception  e) {
            final Duration delay = Duration.ofMillis(applyBackoff());
            nextUpdateAt = Instant.now().plus(delay);
            throw new DownloadFailedException(e.getMessage());
        }
    }

    public void updateDatabaseReader() {
        try {
            downloadDatabases();
            switchDatabase();
            LOG.info("Updated geoip database readers");
            failedAttemptCount.set(0);
        } catch (final Exception e) {
            LOG.error("Failed to download database and create database readers, will try to use old databases if they exist. {}", e.getMessage());
            final Duration delay = Duration.ofMillis(applyBackoff());
            nextUpdateAt = Instant.now().plus(delay);
            final File file = new File(currentDatabaseDir);
            geoIPFileManager.deleteDirectory(file);
            switchDirectory();
        }
    }

    private void switchDatabase() {
        writeLock.lock();
        try {
            final GeoIPDatabaseReader newGeoipDatabaseReader = createReader();
            final GeoIPDatabaseReader oldGeoipDatabaseReader = geoIPDatabaseReader;
            geoIPDatabaseReader = newGeoipDatabaseReader;
            if (oldGeoipDatabaseReader != null) {
                oldGeoipDatabaseReader.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close geoip database readers due to: {}", e.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    private void downloadDatabases() throws Exception {
        DBSource dbSource;
        switchDirectory();

        final String destinationPath = maxMindConfig.getDatabaseDestination() + File.separator + currentDatabaseDir;

        geoIPFileManager.createDirectoryIfNotExist(destinationPath);

        LOG.info("Downloading GeoIP database to {}", destinationPath);

        switch (dbSourceOptions) {
            case HTTP_MANIFEST:
                dbSource = new ManifestDownloadService(destinationPath, maxMindDatabaseConfig);
                dbSource.initiateDownload();
                break;
            case URL:
                dbSource = new HttpDBDownloadService(destinationPath, geoIPFileManager, maxMindDatabaseConfig);
                dbSource.initiateDownload();
                break;
            case S3:
                dbSource = new S3DBService(maxMindConfig.getAwsAuthenticationOptionsConfig(), destinationPath, maxMindDatabaseConfig);
                dbSource.initiateDownload();
                break;
            case PATH:
                dbSource = new LocalDBDownloadService(destinationPath, maxMindDatabaseConfig);
                dbSource.initiateDownload();
                break;
        }
    }

    private GeoIPDatabaseReader createReader() {
        final String finalPath = maxMindConfig.getDatabaseDestination() + File.separator + currentDatabaseDir;
        final LicenseTypeOptions licenseType = licenseTypeCheck.isGeoLite2OrEnterpriseLicense(finalPath);
        if (licenseType == null) {
            throw new NoValidDatabaseFoundException("At least one valid database is required.");
        }
        GeoIPDatabaseReader newGeoIPDatabaseReader;
        if (licenseType.equals(LicenseTypeOptions.FREE)) {
            newGeoIPDatabaseReader = new AutoCountingDatabaseReader(
                    new GeoLite2DatabaseReader(databaseReaderBuilder, geoIPFileManager, finalPath, cacheSize));
        } else if (licenseType.equals(LicenseTypeOptions.ENTERPRISE)) {
            newGeoIPDatabaseReader = new AutoCountingDatabaseReader(
                    new GeoIP2DatabaseReader(databaseReaderBuilder, geoIPFileManager, finalPath, cacheSize));
        } else {
            throw new NoValidDatabaseFoundException("No valid database found to initialize database readers.");
        }
        return newGeoIPDatabaseReader;
    }

    private void switchDirectory() {
        databaseDirToggle = !databaseDirToggle;
        if (databaseDirToggle) {
            currentDatabaseDir = BLUE_DATABASE_DIR;
        } else {
            currentDatabaseDir = GREEN_DATABASE_DIR;
        }
    }

    private long applyBackoff() {
        final long delayMillis = backoff.nextDelayMillis(failedAttemptCount.incrementAndGet());
        if (delayMillis < 0) {
            // retries exhausted
            LOG.info("Retries exhausted to download database. Will retry based on refresh interval");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        LOG.info("Failed to download databases, will retry after {} seconds", delayDuration.getSeconds());
        return delayMillis;
    }

    public GeoIPDatabaseReader getGeoIPDatabaseReader() {
        return geoIPDatabaseReader;
    }

    public Instant getNextUpdateAt() {
        return nextUpdateAt;
    }

    public void setNextUpdateAt(final Instant nextUpdateAt) {
        this.nextUpdateAt = nextUpdateAt;
    }

    public void deleteDatabasesOnShutdown() {
        geoIPFileManager.deleteDirectory(new File(maxMindConfig.getDatabaseDestination() + File.separator + BLUE_DATABASE_DIR));
        geoIPFileManager.deleteDirectory(new File(maxMindConfig.getDatabaseDestination() + File.separator + GREEN_DATABASE_DIR));
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
