/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;
import org.opensearch.dataprepper.plugins.processor.databasedownload.DBSourceOptions;
import org.opensearch.dataprepper.plugins.processor.databasedownload.LicenseTypeOptions;
import org.opensearch.dataprepper.plugins.processor.databasedownload.S3DBService;
import org.opensearch.dataprepper.plugins.processor.databasedownload.DBSource;
import org.opensearch.dataprepper.plugins.processor.databasedownload.HttpDBDownloadService;
import org.opensearch.dataprepper.plugins.processor.databasedownload.LocalDBDownloadService;
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
    private GeoIPProcessorConfig geoIPProcessorConfig;
    private LicenseTypeOptions licenseType;
    private GetGeoData geoData;
    private List<DatabasePathURLConfig> databasePath;
    private final String tempPath;
    private final ScheduledExecutorService scheduledExecutorService;
    private final DBSourceOptions dbSourceOptions;
    public static volatile boolean downloadReady;
    private boolean toggle;
    private String flipDatabase;


    /**
     * GeoIPProcessorService constructor for initialization of required attributes
     * @param geoIPProcessorConfig geoIPProcessorConfig
     * @param tempPath tempPath
     */
    public GeoIPProcessorService(GeoIPProcessorConfig geoIPProcessorConfig, String tempPath) {
        this.toggle = false;
        this.geoIPProcessorConfig = geoIPProcessorConfig;
        this.tempPath = tempPath;
        this.databasePath = geoIPProcessorConfig.getServiceType().getMaxMindService().getDatabasePath();
        flipDatabase = DATABASE_1;

        dbSourceOptions = DbSourceIdentification.getDatabasePathType(databasePath);
        final Duration checkInterval = Objects.requireNonNull(geoIPProcessorConfig.getServiceType().getMaxMindService().getCacheRefreshSchedule());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService
                .scheduleAtFixedRate(this::downloadThroughURLandS3, 0L, checkInterval.toSeconds(), TimeUnit.SECONDS);

        synchronized (this) {
            try {
                while (!downloadReady) {
                    wait();
                }
            } catch (InterruptedException ex) {
                LOG.info("InterruptedException {0} ",  ex);
                Thread.currentThread().interrupt();
            }
            String finalPath = tempPath + File.separator;
            licenseType = LicenseTypeCheck.isGeoLite2OrEnterpriseLicense(finalPath.concat(flipDatabase));
            if (licenseType.equals(LicenseTypeOptions.FREE)) {
                geoData = new GetGeoLite2Data(finalPath.concat(flipDatabase), geoIPProcessorConfig.getServiceType().getMaxMindService().getCacheSize(), geoIPProcessorConfig);
            }
            else if (licenseType.equals(LicenseTypeOptions.ENTERPRISE)) {
                geoData = new GetGeoIP2Data(finalPath.concat(flipDatabase), geoIPProcessorConfig.getServiceType().getMaxMindService().getCacheSize(), geoIPProcessorConfig);
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
                    dbSource.initiateDownload(databasePath);
                    downloadReady = true;
                    break;
                case S3:
                    dbSource = new S3DBService(geoIPProcessorConfig, flipDatabase);
                    dbSource.initiateDownload(databasePath);
                    downloadReady = true;
                    break;
                case PATH:
                    dbSource = new LocalDBDownloadService(tempPath, flipDatabase);
                    dbSource.initiateDownload(databasePath);
                    downloadReady = true;
                    break;
            }
        } catch (Exception ex) {
           throw new DownloadFailedException("Download failed: " + ex);
        }
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
