/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.opensearch.dataprepper.plugins.processor.databasedownload.DBSourceOptions;
import java.net.InetAddress;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * Implementation class of geoIP-processor plugin service class.
 * It is responsible for calling of mmdb files download
 */
public class GeoIPProcessorService {

    /**
     * GeoIPProcessorService constructor for initialization of required attributes
     * @param geoIPProcessorConfig geoIPProcessorConfig
     * @param tempPath tempPath
     */
    public GeoIPProcessorService(GeoIPProcessorConfig geoIPProcessorConfig, String tempPath) {
       //TODO
    }

    /**
     * Calling downlaod method abased on the database path type
     * @param DBSourceOptions DBSourceOptions
     */
    public void downloadThroughURLandS3(DBSourceOptions DBSourceOptions) {
        //TODO
    }

    /**
     * Method to call enrichment of data based on license type
     * @param inetAddress inetAddress
     * @param attributes attributes
     * @param pluginStartDateTime pluginStartDateTime
     * @return Enriched Map
     */
    public Map<String, Object> getGeoData(InetAddress inetAddress, List<String> attributes , ZonedDateTime pluginStartDateTime) {
        //TODO
        return null;
    }
}
