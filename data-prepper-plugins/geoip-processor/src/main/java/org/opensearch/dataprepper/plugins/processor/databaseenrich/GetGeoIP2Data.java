/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;

import java.net.InetAddress;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * Implementation class for enrichment of enterprise data
 */
public class GetGeoIP2Data implements GetGeoData {

    /**
     * GetGeoLite2Data constructor for initialisation of attributes
     * @param dbPath dbPath
     * @param cacheSize cacheSize
     * @param pluginStartDateTime pluginStartDateTime
     * @param geoIPProcessorConfig geoIPProcessorConfig
     */
    public GetGeoIP2Data(String dbPath, int cacheSize, ZonedDateTime pluginStartDateTime, GeoIPProcessorConfig geoIPProcessorConfig) {
        //TODO
        initDatabaseReader();
    }

    /**
     * Initialise all the DatabaseReader
     */
    public void initDatabaseReader() {
        //TODO
    }

    /**
     * Switch all the DatabaseReader
     */
    @Override
    public void switchDatabaseReader() {
        closeReader();
        initDatabaseReader();
    }

    /**
     * Enrich the GeoData
     * @param inetAddress inetAddress
     * @param attributes attributes
     * @return enriched data Map
     */
    @Override
    public Map<String, Object> getGeoData(InetAddress inetAddress, List<String> attributes) {
        //TODO
        return null;
    }


    /**
     * Colse the all DatabaseReader
     */
    @Override
    public void closeReader() {
        //TODO
    }
}
