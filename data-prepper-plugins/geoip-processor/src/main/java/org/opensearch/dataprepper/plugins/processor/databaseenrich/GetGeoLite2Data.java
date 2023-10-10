/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import com.maxmind.db.Network;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Location;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.processor.databasedownload.DBSource;
import org.opensearch.dataprepper.plugins.processor.databasedownload.DatabaseReaderCreate;
import org.opensearch.dataprepper.plugins.processor.loadtype.LoadTypeOptions;
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
 * Implementation class for enrichment of geoip lite2 data
 */
public class GetGeoLite2Data implements GetGeoData {

    private static final Logger LOG = LoggerFactory.getLogger(GetGeoLite2Data.class);
    public static final String COUNTRY_NAME = "country_name";
    public static final String CONTINENT_NAME = "continent_name";
    public static final String REGION_NAME = "region_name";
    public static final String CITY_NAME = "city_name";
    public static final String ORGANIZATION_NAME = "organization_name";
    public static final String NETWORK = "network";
    public static final String COUNTRY_ISO_CODE = "country_iso_code";
    public static final String IP = "ip";
    public static final String REGION_ISO_CODE = "region_iso_code";
    public static final String TIMEZONE = "timezone";
    public static final String LOCATION = "location";
    public static final String ASN = "asn";
    private DatabaseReader.Builder readerCity;
    private DatabaseReader.Builder readerCountry;
    private DatabaseReader.Builder readerAsn;
    private Country country;
    private Continent continent;
    private City city;
    private Location location;
    private Subdivision subdivision;
    private Long asn;
    private String organizationName;
    private Network network;
    private String dbPath;
    private LoadTypeOptions loadType;
    private int cacheSize;
    private CityResponse responseCity;
    private CountryResponse responseCountry;
    private AsnResponse responseAsn;
    private String tempDestDir;


    /**
     * GetGeoLite2Data constructor for initialisation of attributes
     * @param dbPath dbPath
     * @param cacheSize cacheSize
     * @param geoIPProcessorConfig geoIPProcessorConfig
     */
    public GetGeoLite2Data(String dbPath, int cacheSize , GeoIPProcessorConfig geoIPProcessorConfig) {
        this.dbPath = dbPath;
        this.cacheSize = cacheSize;
        this.loadType = geoIPProcessorConfig.getServiceType().getMaxMindService().getLoadType();
        initDatabaseReader();
    }

    /**
     * Initialise all the DatabaseReader
     */
    private void initDatabaseReader() {
        readerCity = DatabaseReaderCreate.createLoader(Path.of(dbPath + File.separator + GeoLite2CityDB), loadType, cacheSize);
        readerCountry = DatabaseReaderCreate.createLoader(Path.of(dbPath + File.separator + GeoLite2CountryDB), loadType, cacheSize);
        readerAsn = DatabaseReaderCreate.createLoader(Path.of(dbPath + File.separator + GeoLite2AsnDB), loadType, cacheSize);
    }

    /**
     * Switch all the DatabaseReader
     */
    @Override
    public void switchDatabaseReader() {
        LOG.info("Switch DatabaseReader");
        closeReaderCity();
        closeReaderCountry();
        closeReaderAsn();
        System.gc();
        File file = new File(dbPath);
        DBSource.deleteDirectory(file);
        dbPath = tempDestDir;
        initDatabaseReader();
    }

    /**
     * Enrich the GeoData
     * @param inetAddress inetAddress
     * @param attributes attributes
     * @return enriched data Map
     */
    @Override
    public Map<String, Object> getGeoData(InetAddress inetAddress, List<String> attributes, String tempDestDir) {
        Map<String, Object> geoData = new HashMap<>();
        if (GeoIPProcessorService.downloadReady) {
            this.tempDestDir = tempDestDir;
            GeoIPProcessorService.downloadReady = false;
            switchDatabaseReader();
        }
        try {
            responseCountry = readerCountry.build().country(inetAddress);
            country = responseCountry.getCountry();
            continent = responseCountry.getContinent();

            responseCity = readerCity.build().city(inetAddress);
            city = responseCity.getCity();
            location = responseCity.getLocation();
            subdivision = responseCity.getMostSpecificSubdivision();

            responseAsn = readerAsn.build().asn(inetAddress);
            asn = responseAsn.getAutonomousSystemNumber();
            organizationName = responseAsn.getAutonomousSystemOrganization();
            network = responseAsn.getNetwork();
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
                        case ASN:
                            if (asn != null) {
                                geoData.put(ASN, asn);
                            }
                            break;
                        case ORGANIZATION_NAME:
                            enrichData(geoData, ORGANIZATION_NAME, organizationName);
                            break;
                        case NETWORK:
                            enrichData(geoData, NETWORK,network!=null? network.toString():null);
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
                enrichData(geoData, TIMEZONE, location.getTimeZone());
                enrichLocationData(geoData, location.getLatitude(), location.getLongitude());

                if (asn != null) {
                    geoData.put(ASN, asn);
                }

                enrichData(geoData, ORGANIZATION_NAME, organizationName);
                enrichData(geoData, NETWORK,network!=null? network.toString():null);
            }
        } catch (Exception ex) {
            throw new EnrichFailedException("Enrichment failed exception" + ex);
        }
        return geoData;
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
        try {
            if (readerCity != null)
                readerCity.build().close();
            readerCity = null;
        } catch (IOException ex) {
            LOG.info("Close City DatabaseReader Exception : {0}",  ex);
        }
    }

    /**
     * Close the Country DatabaseReader
     */
    private void closeReaderCountry(){
        try {
            if (readerCountry != null)
                readerCountry.build().close();
            readerCountry = null;
        } catch (IOException ex) {
            LOG.info("Close Country DatabaseReader Exception : {0}",  ex);
        }
    }

    /**
     * Close the ASN DatabaseReader
     */
    private void closeReaderAsn() {
        try {
            if (readerAsn != null)
                readerAsn.build().close();
            readerAsn = null;
        } catch (IOException ex) {
          LOG.info("Close Asn DatabaseReader Exception : {0}",  ex);
        }
    }
}
