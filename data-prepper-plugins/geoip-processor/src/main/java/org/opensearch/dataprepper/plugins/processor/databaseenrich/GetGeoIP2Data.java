/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.EnterpriseResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import org.opensearch.dataprepper.plugins.processor.exception.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DatabaseReaderCreate;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation class for enrichment of enterprise data
 */
public class GetGeoIP2Data implements GetGeoData {

    private static final Logger LOG = LoggerFactory.getLogger(GetGeoIP2Data.class);
    public static final String COUNTRY_NAME = "country_name";
    public static final String CONTINENT_NAME = "continent_name";
    public static final String REGION_NAME = "region_name";
    public static final String CITY_NAME = "city_name";
    public static final String COUNTRY_ISO_CODE = "country_iso_code";
    public static final String IP = "ip";
    public static final String REGION_ISO_CODE = "region_iso_code";
    public static final String TIMEZONE = "timezone";
    public static final String LOCATION = "location";
    public static final String POSTAL = "postal";
    private DatabaseReader readerEnterprise;
    private Country country;
    private Continent continent;
    private City city;
    private Location location;
    private Subdivision subdivision;
    private String dbPath;
    private int cacheSize;
    private Postal postal;
    private String tempDestDir;

    /**
     * GetGeoLite2Data constructor for initialisation of attributes
     * @param dbPath dbPath
     * @param cacheSize cacheSize
     */
    public GetGeoIP2Data(final String dbPath, final int cacheSize) {
        this.dbPath = dbPath;
        this.cacheSize = cacheSize;
        initDatabaseReader();
    }

    /**
     * Initialise all the DatabaseReader
     */
    public void initDatabaseReader() {
        try {
            readerEnterprise = DatabaseReaderCreate.buildReader(Path.of(dbPath + File.separator + GeoIP2EnterpriseDB), cacheSize);
        } catch (final IOException ex) {
            LOG.error("Exception while creating GeoIP2 DatabaseReader: {0}", ex);
        }
    }

    /**
     * Switch all the DatabaseReader
     */
    @Override
    public void switchDatabaseReader() {
        LOG.info("Switching GeoIP2 DatabaseReader");
        closeReader();
        System.gc();
        File file = new File(dbPath);
        DBSource.deleteDirectory(file);
        initDatabaseReader();
    }

    /**
     * Enrich the GeoData
     * @param inetAddress inetAddress
     * @param attributes attributes
     * @return enriched data Map
     */
    public Map<String, Object> getGeoData(InetAddress inetAddress, List<String> attributes) {
        Map<String, Object> geoData = new HashMap<>();
        try {
            EnterpriseResponse enterpriseResponse = readerEnterprise.enterprise(inetAddress);
            country = enterpriseResponse.getCountry();
            subdivision = enterpriseResponse.getMostSpecificSubdivision();
            city = enterpriseResponse.getCity();
            location = enterpriseResponse.getLocation();
            continent = enterpriseResponse.getContinent();
            postal = enterpriseResponse.getPostal();
        } catch (IOException | GeoIp2Exception ex) {
            LOG.info("Look up Exception : {0}",  ex);
        }

        try {
            if ((attributes != null) && (!attributes.isEmpty())) {
                for (String attribute : attributes) {
                    switch (attribute) {
                        case IP:
                            enrichData(geoData, IP, inetAddress.getHostAddress());
                            break;
                        case COUNTRY_ISO_CODE:
                            enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
                            break;
                        case COUNTRY_NAME:
                            enrichData(geoData, COUNTRY_NAME, country.getName());
                            break;
                        case CONTINENT_NAME:
                            enrichData(geoData, CONTINENT_NAME, continent.getName());
                            break;
                        case REGION_ISO_CODE:
                            // ISO 3166-2 code for country subdivisions.
                            // See iso.org/iso-3166-country-codes.html
                            enrichRegionIsoCode(geoData, country.getIsoCode(), subdivision.getIsoCode());
                            break;
                        case REGION_NAME:
                            enrichData(geoData, REGION_NAME, subdivision.getName());
                            break;
                        case CITY_NAME:
                            enrichData(geoData, CITY_NAME, city.getName());
                            break;
                        case TIMEZONE:
                            enrichData(geoData, TIMEZONE, location.getTimeZone());
                            break;
                        case LOCATION:
                            enrichLocationData(geoData, location.getLatitude(), location.getLongitude());
                            break;
                        case POSTAL:
                            enrichData(geoData, "postalCode", postal.getCode());
                            break;
                    }
                }
            } else {

                enrichData(geoData, IP, inetAddress.getHostAddress());
                enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
                enrichData(geoData, COUNTRY_NAME, country.getName());
                enrichData(geoData, CONTINENT_NAME, continent.getName());

                enrichRegionIsoCode(geoData, country.getIsoCode(), subdivision.getIsoCode());

                enrichData(geoData, REGION_NAME, subdivision.getName());
                enrichData(geoData, CITY_NAME, city.getName());
                enrichData(geoData, "postalCode", postal.getCode());

                enrichData(geoData, TIMEZONE, location.getTimeZone());
                enrichLocationData(geoData, location.getLatitude(), location.getLongitude());
            }
        } catch (Exception ex) {
            throw new EnrichFailedException("Enrichment failed exception" + ex);
        }
        return geoData;
    }


    /**
     * Close the DatabaseReader
     */
    @Override
    public void closeReader() {
        try {
            if (readerEnterprise != null)
                readerEnterprise.close();
        } catch (IOException ex) {
            LOG.info("Close Enterprise DatabaseReader Exception : {0}",  ex);
        }
    }
}
