/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.EnterpriseResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;
import org.opensearch.dataprepper.plugins.processor.exception.DatabaseReaderInitializationException;
import org.opensearch.dataprepper.plugins.processor.exception.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.exception.NoValidDatabaseFoundException;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DatabaseReaderCreate;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class GeoIP2DatabaseReader implements GeoIPDatabaseReader, AutoCloseable {
    private static final String MAXMIND_GEOIP2_DATABASE_TYPE = "geoip2";
    private static final String ENTERPRISE_DATABASE = "enterprise";
    private final String databasePath;
    private final int cacheSize;
    private final AtomicInteger closeCount;
    private DatabaseReader enterpriseDatabaseReader;

    public GeoIP2DatabaseReader(final String databasePath, final int cacheSize) {
        this.databasePath = databasePath;
        this.cacheSize = cacheSize;
        closeCount = new AtomicInteger(1);
        buildDatabaseReaders();
    }

    private void buildDatabaseReaders() {
        try {
            final Optional<String> enterpriseDatabaseName = getDatabaseName(ENTERPRISE_DATABASE, databasePath, MAXMIND_GEOIP2_DATABASE_TYPE);

            if (enterpriseDatabaseName.isPresent()) {
                enterpriseDatabaseReader = DatabaseReaderCreate.buildReader(Path.of(databasePath + File.separator + enterpriseDatabaseName), cacheSize);
            }
        } catch (final IOException ex) {
            throw new DatabaseReaderInitializationException("Exception while creating GeoIP2 DatabaseReaders due to: " + ex.getMessage());
        }

        if (enterpriseDatabaseReader == null) {
            throw new NoValidDatabaseFoundException("Unable to initialize GeoIP2 database, make sure it is valid.");
        }
    }
    @Override
    public Map<String, Object> getGeoData(final InetAddress inetAddress, final List<String> fields, final Set<GeoIPDatabase> geoIPDatabases) {
        Map<String, Object> geoData = new HashMap<>();

        try {
            final Optional<EnterpriseResponse> optionalEnterpriseResponse = enterpriseDatabaseReader.tryEnterprise(inetAddress);
            optionalEnterpriseResponse.ifPresent(response -> processEnterpriseResponse(response, geoData, fields));

            final Optional<AsnResponse> asnResponse = enterpriseDatabaseReader.tryAsn(inetAddress);
            asnResponse.ifPresent(response -> processAsnResponse(response, geoData, fields));

        } catch (final IOException | GeoIp2Exception e) {
            throw new EnrichFailedException("Address not found in database.");
        }
        enrichData(geoData, IP, inetAddress.getHostAddress());
        return geoData;
    }

    private void processAsnResponse(final AsnResponse asnResponse, final Map<String, Object> geoData, final List<String> fields) {
        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case ASN:
                        if (asnResponse.getAutonomousSystemNumber() != null) {
                            enrichData(geoData, ASN, asnResponse.getAutonomousSystemNumber());
                        }
                        break;
                    case ORGANIZATION:
                        enrichData(geoData, ORGANIZATION, asnResponse.getAutonomousSystemOrganization());
                        break;
                    case NETWORK:
                        enrichData(geoData, NETWORK, asnResponse.getNetwork().toString());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, ASN, asnResponse.getAutonomousSystemNumber());
            enrichData(geoData, ORGANIZATION, asnResponse.getAutonomousSystemOrganization());
            enrichData(geoData, NETWORK, asnResponse.getNetwork().toString());
        }
    }

    private void processEnterpriseResponse(final EnterpriseResponse enterpriseResponse, final Map<String, Object> geoData, final List<String> fields) {
        final Continent continent = enterpriseResponse.getContinent();
        final Country country = enterpriseResponse.getCountry();
        final City city = enterpriseResponse.getCity();
        final Location location = enterpriseResponse.getLocation();
        final Postal postal = enterpriseResponse.getPostal();
        final Subdivision mostSpecificSubdivision = enterpriseResponse.getMostSpecificSubdivision();

        final Map<String, Object> locationObject = new HashMap<>();
        locationObject.put(LATITUDE, location.getLatitude());
        locationObject.put(LONGITUDE, location.getLongitude());

        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case CONTINENT_CODE:
                        enrichData(geoData, CONTINENT_CODE, continent.getCode());
                        break;
                    case CONTINENT_NAME:
                        enrichData(geoData, CONTINENT_NAME, continent.getName());
                        break;
                    case COUNTRY_NAME:
                        enrichData(geoData, COUNTRY_NAME, country.getName());
                        break;
                    case IS_COUNTRY_IN_EUROPEAN_UNION:
                        enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
                        break;
                    case COUNTRY_ISO_CODE:
                        enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
                        break;
                    case COUNTRY_CONFIDENCE:
                        enrichData(geoData, COUNTRY_CONFIDENCE, country.getConfidence());
                        break;
                    case CITY_NAME:
                        enrichData(geoData, CITY_NAME, city.getName());
                        break;
                    case CITY_CONFIDENCE:
                        enrichData(geoData, CITY_CONFIDENCE, city.getConfidence());
                        break;
                    case LATITUDE:
                        enrichData(geoData, LATITUDE, location.getLatitude());
                        break;
                    case LONGITUDE:
                        enrichData(geoData, LONGITUDE, location.getLongitude());
                        break;
                    case LOCATION:
                        enrichData(geoData, LOCATION, locationObject);
                        break;
                    case LOCATION_ACCURACY_RADIUS:
                        enrichData(geoData, LOCATION_ACCURACY_RADIUS, location.getAccuracyRadius());
                        break;
                    case METRO_CODE:
                        enrichData(geoData, METRO_CODE, location.getMetroCode());
                        break;
                    case TIME_ZONE:
                        enrichData(geoData, TIME_ZONE, location.getTimeZone());
                        break;
                    case POSTAL_CODE:
                        enrichData(geoData, POSTAL_CODE, postal.getCode());
                        break;
                    case POSTAL_CODE_CONFIDENCE:
                        enrichData(geoData, POSTAL_CODE_CONFIDENCE, postal.getConfidence());
                        break;
                    case MOST_SPECIFIED_SUBDIVISION_ISO_CODE:
                        enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, mostSpecificSubdivision.getIsoCode());
                        break;
                    case MOST_SPECIFIED_SUBDIVISION_NAME:
                        enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_NAME, mostSpecificSubdivision.getName());
                        break;
                    case MOST_SPECIFIED_SUBDIVISION_CONFIDENCE:
                        enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_CONFIDENCE, mostSpecificSubdivision.getConfidence());
                        break;
                }
            }
        } else{
            // add all fields - latitude & longitude will be part of location key
            enrichData(geoData, CONTINENT_CODE, continent.getCode());
            enrichData(geoData, CONTINENT_NAME, continent.getName());
            enrichData(geoData, COUNTRY_NAME, country.getName());
            enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
            enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
            enrichData(geoData, COUNTRY_CONFIDENCE, country.getConfidence());
            enrichData(geoData, CITY_NAME, city.getName());
            enrichData(geoData, CITY_CONFIDENCE, city.getConfidence());
            enrichData(geoData, LOCATION, locationObject);
            enrichData(geoData, LOCATION_ACCURACY_RADIUS, location.getAccuracyRadius());
            enrichData(geoData, METRO_CODE, location.getMetroCode());
            enrichData(geoData, TIME_ZONE, location.getTimeZone());
            enrichData(geoData, POSTAL_CODE, postal.getCode());
            enrichData(geoData, POSTAL_CODE_CONFIDENCE, postal.getConfidence());
            enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, mostSpecificSubdivision.getIsoCode());
            enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_NAME, mostSpecificSubdivision.getName());
            enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_CONFIDENCE, mostSpecificSubdivision.getConfidence());
        }

    }


    @Override
    public boolean areDatabasesExpired() {
        return false;
    }

    @Override
    public void retain() {

    }

    @Override
    public void close() {

    }
}
