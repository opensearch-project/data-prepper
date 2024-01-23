/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSourceOptions;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LicenseTypeOptions;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.S3DBService;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSource;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.HttpDBDownloadService;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LocalDBDownloadService;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.DownloadFailedException;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoData;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoIP2Data;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoLite2Data;
import org.opensearch.dataprepper.plugins.processor.utils.DbSourceIdentification;
import org.opensearch.dataprepper.plugins.processor.utils.LicenseTypeCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Implementation class of geoIP-processor plugin service class.
 * It is responsible for calling of mmdb files download
 */
public class GeoIPProcessorService {

    private static final Logger LOG = LoggerFactory.getLogger(GeoIPProcessorService.class);
    public static final String DATABASE_1 = "first_database_path";
    public static final String DATABASE_2 = "second_database_path";
    private static final String TEMP_PATH_FOLDER = "GeoIP";
    private LicenseTypeOptions licenseType;
    private GetGeoData geoData;
    private List<String> databasePaths;
    private final String tempPath;
    private final ScheduledExecutorService scheduledExecutorService;
    private final DBSourceOptions dbSourceOptions;
    private final MaxMindConfig maxMindConfig;
    public static volatile boolean downloadReady;
    private boolean toggle;
    private String flipDatabase;
    private boolean isDuringInitialization;

    /**
     * GeoIPProcessorService constructor for initialization of required attributes
     *
     * @param geoIpServiceConfig geoIpServiceConfig
     */
    public GeoIPProcessorService(final GeoIpServiceConfig geoIpServiceConfig) {
        this.toggle = false;
        this.maxMindConfig = geoIpServiceConfig.getMaxMindConfig();
        this.databasePaths = maxMindConfig.getDatabasePaths();
        this.isDuringInitialization = true;
        flipDatabase = DATABASE_1;

        this.tempPath = System.getProperty("java.io.tmpdir")+ File.separator + TEMP_PATH_FOLDER;

        dbSourceOptions = DbSourceIdentification.getDatabasePathType(databasePaths);
        final Duration checkInterval = Objects.requireNonNull(maxMindConfig.getDatabaseRefreshInterval());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService
                .scheduleAtFixedRate(this::downloadThroughURLandS3, 0L, checkInterval.toSeconds(), TimeUnit.SECONDS);

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
                String finalPath = tempPath + File.separator;
                licenseType = LicenseTypeCheck.isGeoLite2OrEnterpriseLicense(finalPath.concat(flipDatabase));
                if (licenseType.equals(LicenseTypeOptions.FREE)) {
                    geoData = new GetGeoLite2Data(finalPath.concat(flipDatabase), maxMindConfig.getCacheSize());
                } else if (licenseType.equals(LicenseTypeOptions.ENTERPRISE)) {
                    geoData = new GetGeoIP2Data(finalPath.concat(flipDatabase), maxMindConfig.getCacheSize());
                }
            }
        }
        downloadReady = false;
    }

    /**
     * Calling download method based on the database path type
     */
    public synchronized void downloadThroughURLandS3() {
        DBSource dbSource;
        toggle = !toggle;
        if (!toggle) {
            flipDatabase = DATABASE_1;
        } else {
            flipDatabase = DATABASE_2;
        }

        try {
            switch (dbSourceOptions) {
                case URL:
                    dbSource = new HttpDBDownloadService(flipDatabase);
                    dbSource.initiateDownload(databasePaths);
                    downloadReady = true;
                    break;
                case S3:
                    dbSource = new S3DBService(maxMindConfig.getAwsAuthenticationOptionsConfig(), flipDatabase);
                    dbSource.initiateDownload(databasePaths);
                    downloadReady = true;
                    break;
                case PATH:
                    dbSource = new LocalDBDownloadService(tempPath, flipDatabase);
                    dbSource.initiateDownload(databasePaths);
                    downloadReady = true;
                    break;
            }
        } catch (final Exception ex) {
            if (isDuringInitialization) {
                throw new DownloadFailedException("Download failed due to: " + ex);
            } else {
                LOG.error("Download failed due to: {0}. Using previously loaded database files.", ex);
            }
        }
        isDuringInitialization = false;
        notifyAll();
    }

    /**
     * Method to call enrichment of data based on license type
     * @param inetAddress inetAddress
     * @param attributes attributes
     * @return Enriched Map
     */
    public Map<String, Object> getGeoData(InetAddress inetAddress, List<String> attributes) {
        return geoData.getGeoData(inetAddress, attributes, tempPath +  File.separator + flipDatabase);
    }
}
