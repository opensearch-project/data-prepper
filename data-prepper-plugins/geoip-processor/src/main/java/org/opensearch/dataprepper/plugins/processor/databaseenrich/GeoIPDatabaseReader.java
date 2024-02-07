/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;

import java.io.File;
import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Interface for storing and maintaining MaxMind database readers
 */

public interface GeoIPDatabaseReader {
    String MAXMIND_DATABASE_EXTENSION = ".mmdb";
    String CONTINENT_CODE = "continent_code";
    String CONTINENT_NAME = "continent_name";
    String COUNTRY_ISO_CODE = "country_iso_code";
    String COUNTRY_NAME = "country_name";
    String COUNTRY_CONFIDENCE = "country_confidence";
    String IS_COUNTRY_IN_EUROPEAN_UNION = "is_country_in_european_union";
    String CITY_NAME = "city_name";
    String CITY_CONFIDENCE = "city_confidence";
    String MOST_SPECIFIED_SUBDIVISION_NAME = "most_specified_subdivision_name";
    String MOST_SPECIFIED_SUBDIVISION_ISO_CODE = "most_specified_subdivision_iso_code";
    String MOST_SPECIFIED_SUBDIVISION_CONFIDENCE = "most_specified_subdivision_confidence";
    String NETWORK = "network";
    String IP = "ip";
    String TIME_ZONE = "time_zone";
    String LOCATION = "location";
    String LOCATION_ACCURACY_RADIUS = "location_accuracy_radius";
    String LATITUDE = "latitude";
    String LONGITUDE = "longitude";
    String METRO_CODE = "metro_code";
    String POSTAL_CODE = "postal_code";
    String POSTAL_CODE_CONFIDENCE = "postal_code_confidence";
    String ASN = "asn";
    String ORGANIZATION = "organization";
    Duration MAX_EXPIRY_DURATION = Duration.ofDays(30);

    /**
     * Gets the geo data from the {@link com.maxmind.geoip2.DatabaseReader}
     *
     * @param inetAddress InetAddress
     * @return Map of geo field and value pairs from IP address
     *
     * @since 2.7
     */
    Map<String, Object> getGeoData(InetAddress inetAddress, List<String> fields, Set<GeoIPDatabase> geoIPDatabases);

    /**
     * Gets if the database is expired from metadata or last updated timestamp
     *
     * @return boolean indicating if database is expired
     *
     * @since 2.7
     */
    boolean areDatabasesExpired();

    /**
     * Retains the reader which prevents from closing if it's being used
     *
     * @since 2.7
     */
    void retain();

    /**
     * Closes the reader after processing a batch
     *
     * @since 2.7
     */
    void close();

    /**
     * Enrich attributes
     * @param geoData geoData
     * @param fieldName fieldName
     * @param fieldValue fieldValue
     */
    default void enrichData(final Map<String, Object> geoData, final String fieldName, final Object fieldValue) {
        if (!geoData.containsKey(fieldName) && fieldValue != null) {
            geoData.put(fieldName, fieldValue);
        }
    }

    default Optional<String> getDatabaseName(final String database, final String databasePath, final String databaseType) {
        final File file = new File(databasePath);
        if (file.exists() && file.isDirectory()) {
            final String[] list = file.list();
            for (final String fileName: list) {
                final String lowerCaseFileName = fileName.toLowerCase();
                if (lowerCaseFileName.contains(database)
                        && fileName.endsWith(MAXMIND_DATABASE_EXTENSION)
                        && lowerCaseFileName.contains(databaseType)) {
                    return Optional.of(fileName);
                }
            }
        }
        return Optional.empty();
    }

    default Map<String, Object> extractContinentFields(final Continent continent, final Map<String, Object> geoData, final List<String> fields) {
        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case CONTINENT_CODE:
                        enrichData(geoData, CONTINENT_CODE, continent.getCode());
                        break;
                    case CONTINENT_NAME:
                        enrichData(geoData, CONTINENT_NAME, continent.getName());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, CONTINENT_CODE, continent.getCode());
            enrichData(geoData, CONTINENT_NAME, continent.getName());
        }
        return geoData;
    }

    default Map<String, Object> extractCountryFields(final Country country, final Map<String, Object> geoData, final List<String> fields) {
        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case COUNTRY_NAME:
                        enrichData(geoData, CONTINENT_NAME, country.getName());
                        break;
                    case IS_COUNTRY_IN_EUROPEAN_UNION:
                        enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
                        break;
                    case COUNTRY_ISO_CODE:
                        enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, CONTINENT_NAME, country.getName());
            enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
            enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
        }
        return geoData;
    }

    default Map<String, Object> extractCityFields(final City city, final Map<String, Object> geoData, final List<String> fields) {
        if (!fields.isEmpty()) {
            for (final String field : fields) {
                if (field.equals(CITY_NAME)) {
                    enrichData(geoData, CITY_NAME, city.getName());
                }
            }
        } else{
            enrichData(geoData, CITY_NAME, city.getName());
        }
        return geoData;
    }

    default Map<String, Object> extractLocationFields(final Location location, final Map<String, Object> geoData, final List<String> fields) {
        final Map<String, Object> locationObject = new HashMap<>();
        locationObject.put(LATITUDE, location.getLatitude());
        locationObject.put(LONGITUDE, location.getLongitude());

        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case LOCATION:
                        enrichData(geoData, LOCATION, locationObject);
                        break;
                    case LATITUDE:
                        enrichData(geoData, LATITUDE, location.getLatitude());
                        break;
                    case LONGITUDE:
                        enrichData(geoData, LONGITUDE, location.getLongitude());
                        break;
                    case METRO_CODE:
                        enrichData(geoData, METRO_CODE, location.getMetroCode());
                        break;
                    case TIME_ZONE:
                        enrichData(geoData, TIME_ZONE, location.getTimeZone());
                        break;
                }
            }
        } else{
            // add all fields - latitude & longitude will be part of location key
            enrichData(geoData, LOCATION, locationObject);
            enrichData(geoData, METRO_CODE, location.getMetroCode());
            enrichData(geoData, TIME_ZONE, location.getTimeZone());
        }
        return geoData;
    }

    default Map<String, Object> extractPostalFields(final Postal postal, final Map<String, Object> geoData, final List<String> fields) {
        if (!fields.isEmpty()) {
            for (final String field : fields) {
                if (field.equals(POSTAL_CODE)) {
                    enrichData(geoData, POSTAL_CODE, postal.getCode());
                }
            }
        } else{
            enrichData(geoData, POSTAL_CODE, postal.getCode());
        }
        return geoData;
    }

    default Map<String, Object> extractSubdivisionFields(final Subdivision subdivision, final Map<String, Object> geoData, final List<String> fields) {
        if (!fields.isEmpty()) {
            for (final String field : fields) {
                if (field.equals(MOST_SPECIFIED_SUBDIVISION_NAME)) {
                    enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_NAME, subdivision.getName());
                } else if (field.equals(MOST_SPECIFIED_SUBDIVISION_ISO_CODE)) {
                    enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, subdivision.getIsoCode());
                }
            }
        } else{
            enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_NAME, subdivision.getName());
            enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, subdivision.getIsoCode());
        }
        return geoData;
    }


}
