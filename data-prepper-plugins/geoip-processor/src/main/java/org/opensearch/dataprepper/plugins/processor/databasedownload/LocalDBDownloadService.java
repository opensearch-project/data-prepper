/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;

import java.util.List;


/**
 * Implementation class for Download through local path
 */
public class LocalDBDownloadService implements DBSource {

    /**
     * LocalDBDownloadService constructor for initialisation of attributes
     * @param geoIPProcessorConfig geoIPProcessorConfig
     * @param tempPath tempPath
     */
    public LocalDBDownloadService(GeoIPProcessorConfig geoIPProcessorConfig, String tempPath) {
        //TODO
    }

    /**
     * Initialisation of Download from local file path
     * @param config config
     */
    @Override
    public void initiateDownload(List<DatabasePathURLConfig> config) {
        //TODO : Initialisation of Download from local file path
    }

    /**
     * Build Request And DownloadFile
     * @param key key
     */
    @Override
    public void buildRequestAndDownloadFile(String key) {
        //TODO : Build Request And DownloadFile
    }

}
