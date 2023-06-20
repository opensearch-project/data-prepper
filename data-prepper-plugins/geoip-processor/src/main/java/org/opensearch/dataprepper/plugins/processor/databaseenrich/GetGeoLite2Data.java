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
 * Implementation class for enrichment of geolite data
 */
public class GetGeoLite2Data implements GetGeoData {

    /**
     * GetGeoLite2Data constructor for initialisation of attributes
     * @param dbPath dbPath
     * @param cacheSize cacheSize
     * @param pluginStartDateTime pluginStartDateTime
     * @param geoIPProcessorConfig geoIPProcessorConfig
     */
    public GetGeoLite2Data(String dbPath, int cacheSize , ZonedDateTime pluginStartDateTime, GeoIPProcessorConfig geoIPProcessorConfig) {
        //TODO
        initDatabaseReader();
    }

    /**
     * Initialise all the DatabaseReader
     */
    private void initDatabaseReader() {
        //TODO
    }

    /**
     * Switch all the DatabaseReader
     */
    @Override
    public void switchDatabaseReader() {
        closeReaderCity();
        closeReaderCountry();
        closeReaderAsn();

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
     * Close the all DatabaseReader
     */
    @Override
    public void closeReader() {
        closeReaderCity();
        closeReaderCountry();
        closeReaderAsn();
    }

    /**
     * Close the City DatabaseReader
     */
    private void closeReaderCity(){
        //TODO
    }

    /**
     * Close the Country DatabaseReader
     */
    private void closeReaderCountry(){
        //TODO
    }

    /**
     * Close the ASN DatabaseReader
     */
    private void closeReaderAsn() {
        //TODO
    }
}
