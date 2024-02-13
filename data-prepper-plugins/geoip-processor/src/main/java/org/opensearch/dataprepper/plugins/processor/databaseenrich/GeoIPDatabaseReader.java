/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.RepresentedCountry;
import com.maxmind.geoip2.record.Subdivision;
import org.opensearch.dataprepper.plugins.processor.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.processor.GeoIPField;

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
    Duration MAX_EXPIRY_DURATION = Duration.ofDays(30);
    String LAT = "lat";
    String LON = "lon";

    /**
     * Gets the geo data from the {@link com.maxmind.geoip2.DatabaseReader}
     *
     * @param inetAddress InetAddress
     * @return Map of geo field and value pairs from IP address
     *
     * @since 2.7
     */
    Map<String, Object> getGeoData(InetAddress inetAddress, List<GeoIPField> fields, Set<GeoIPDatabase> geoIPDatabases);

    /**
     * Gets if the database is expired from metadata or last updated timestamp
     *
     * @return boolean indicating if database is expired
     *
     * @since 2.7
     */
    boolean isExpired();

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

    default void extractContinentFields(final Continent continent,
                                        final Map<String, Object> geoData,
                                        final List<GeoIPField> fields) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case CONTINENT_CODE:
                        enrichData(geoData, GeoIPField.CONTINENT_CODE.getFieldName(), continent.getCode());
                        break;
                    case CONTINENT_NAME:
                        enrichData(geoData, GeoIPField.CONTINENT_NAME.getFieldName(), continent.getName());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, GeoIPField.CONTINENT_CODE.getFieldName(), continent.getCode());
            enrichData(geoData, GeoIPField.CONTINENT_NAME.getFieldName(), continent.getName());
        }
    }

    default void extractCountryFields(final Country country,
                                      final Map<String, Object> geoData,
                                      final List<GeoIPField> fields,
                                      final boolean isEnterpriseDatabase) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case COUNTRY_NAME:
                        enrichData(geoData, GeoIPField.COUNTRY_NAME.getFieldName(), country.getName());
                        break;
                    case IS_COUNTRY_IN_EUROPEAN_UNION:
                        enrichData(geoData, GeoIPField.IS_COUNTRY_IN_EUROPEAN_UNION.getFieldName(), country.isInEuropeanUnion());
                        break;
                    case COUNTRY_ISO_CODE:
                        enrichData(geoData, GeoIPField.COUNTRY_ISO_CODE.getFieldName(), country.getIsoCode());
                        break;
                    case COUNTRY_CONFIDENCE:
                        if (isEnterpriseDatabase)
                            enrichData(geoData, GeoIPField.COUNTRY_CONFIDENCE.getFieldName(), country.getConfidence());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, GeoIPField.COUNTRY_NAME.getFieldName(), country.getName());
            enrichData(geoData, GeoIPField.IS_COUNTRY_IN_EUROPEAN_UNION.getFieldName(), country.isInEuropeanUnion());
            enrichData(geoData, GeoIPField.COUNTRY_ISO_CODE.getFieldName(), country.getIsoCode());
            if (isEnterpriseDatabase)
                enrichData(geoData,GeoIPField. COUNTRY_CONFIDENCE.getFieldName(), country.getConfidence());
        }
    }

    default void extractRegisteredCountryFields(final Country registeredCountry,
                                                final Map<String, Object> geoData,
                                                final List<GeoIPField> fields) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case REGISTERED_COUNTRY_NAME:
                        enrichData(geoData, GeoIPField.REGISTERED_COUNTRY_NAME.getFieldName(), registeredCountry.getName());
                        break;
                    case REGISTERED_COUNTRY_ISO_CODE:
                        enrichData(geoData, GeoIPField.REGISTERED_COUNTRY_ISO_CODE.getFieldName(), registeredCountry.getIsoCode());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, GeoIPField.REGISTERED_COUNTRY_NAME.getFieldName(), registeredCountry.getName());
            enrichData(geoData, GeoIPField.REGISTERED_COUNTRY_ISO_CODE.getFieldName(), registeredCountry.getIsoCode());
        }
    }

    default void extractRepresentedCountryFields(final RepresentedCountry representedCountry,
                                                     final Map<String, Object> geoData,
                                                     final List<GeoIPField> fields) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case REPRESENTED_COUNTRY_NAME:
                        enrichData(geoData, GeoIPField.REPRESENTED_COUNTRY_NAME.getFieldName(), representedCountry.getName());
                        break;
                    case REPRESENTED_COUNTRY_ISO_CODE:
                        enrichData(geoData, GeoIPField.REPRESENTED_COUNTRY_ISO_CODE.getFieldName(), representedCountry.getIsoCode());
                        break;
                    case REPRESENTED_COUNTRY_TYPE:
                        enrichData(geoData, GeoIPField.REPRESENTED_COUNTRY_TYPE.getFieldName(), representedCountry.getType());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, GeoIPField.REPRESENTED_COUNTRY_NAME.getFieldName(), representedCountry.getName());
            enrichData(geoData, GeoIPField.REPRESENTED_COUNTRY_ISO_CODE.getFieldName(), representedCountry.getIsoCode());
            enrichData(geoData, GeoIPField.REPRESENTED_COUNTRY_TYPE.getFieldName(), representedCountry.getType());
        }
    }

    default void extractCityFields(final City city,
                                   final Map<String, Object> geoData,
                                   final List<GeoIPField> fields,
                                   final boolean isEnterpriseDatabase) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                if (field.equals(GeoIPField.CITY_NAME)) {
                    enrichData(geoData, GeoIPField.CITY_NAME.getFieldName(), city.getName());
                } else if (isEnterpriseDatabase && field.equals(GeoIPField.CITY_CONFIDENCE)) {
                    enrichData(geoData, GeoIPField.CITY_CONFIDENCE.getFieldName(), city.getConfidence());
                }
            }
        } else{
            enrichData(geoData, GeoIPField.CITY_NAME.getFieldName(), city.getName());
            if (isEnterpriseDatabase)
                enrichData(geoData, GeoIPField.CITY_CONFIDENCE.getFieldName(), city.getConfidence());
        }
    }

    default void extractLocationFields(final Location location,
                                       final Map<String, Object> geoData,
                                       final List<GeoIPField> fields) {
        final Map<String, Object> locationObject = new HashMap<>();
        locationObject.put(LAT, location.getLatitude());
        locationObject.put(LON, location.getLongitude());

        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case LOCATION:
                        enrichData(geoData, GeoIPField.LOCATION.getFieldName(), locationObject);
                        break;
                    case LATITUDE:
                        enrichData(geoData, GeoIPField.LATITUDE.getFieldName(), location.getLatitude());
                        break;
                    case LONGITUDE:
                        enrichData(geoData, GeoIPField.LONGITUDE.getFieldName(), location.getLongitude());
                        break;
                    case METRO_CODE:
                        enrichData(geoData, GeoIPField.METRO_CODE.getFieldName(), location.getMetroCode());
                        break;
                    case TIME_ZONE:
                        enrichData(geoData, GeoIPField.TIME_ZONE.getFieldName(), location.getTimeZone());
                        break;
                    case LOCATION_ACCURACY_RADIUS:
                        enrichData(geoData, GeoIPField.LOCATION_ACCURACY_RADIUS.getFieldName(), location.getAccuracyRadius());
                        break;
                }
            }
        } else{
            // add all fields - latitude & longitude will be part of location key
            enrichData(geoData, GeoIPField.LOCATION.getFieldName(), locationObject);
            enrichData(geoData, GeoIPField.METRO_CODE.getFieldName(), location.getMetroCode());
            enrichData(geoData, GeoIPField.TIME_ZONE.getFieldName(), location.getTimeZone());
            enrichData(geoData, GeoIPField.LOCATION_ACCURACY_RADIUS.getFieldName(), location.getAccuracyRadius());
        }
    }

    default void extractPostalFields(final Postal postal,
                                                    final Map<String, Object> geoData,
                                                    final List<GeoIPField> fields,
                                                    final boolean isEnterpriseDatabase) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                if (field.equals(GeoIPField.POSTAL_CODE)) {
                    enrichData(geoData, GeoIPField.POSTAL_CODE.getFieldName(), postal.getCode());
                } else if (isEnterpriseDatabase && field.equals(GeoIPField.POSTAL_CODE_CONFIDENCE)) {
                    enrichData(geoData, GeoIPField.POSTAL_CODE_CONFIDENCE.getFieldName(), postal.getConfidence());
                }
            }
        } else{
            enrichData(geoData, GeoIPField.POSTAL_CODE.getFieldName(), postal.getCode());
            if (isEnterpriseDatabase)
                enrichData(geoData, GeoIPField.POSTAL_CODE_CONFIDENCE.getFieldName(), postal.getConfidence());
        }
    }

    default void extractMostSpecifiedSubdivisionFields(final Subdivision subdivision,
                                                         final Map<String, Object> geoData,
                                                         final List<GeoIPField> fields,
                                                         final boolean isEnterpriseDatabase) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case MOST_SPECIFIED_SUBDIVISION_NAME:
                        enrichData(geoData, GeoIPField.MOST_SPECIFIED_SUBDIVISION_NAME.getFieldName(), subdivision.getName());
                        break;
                    case MOST_SPECIFIED_SUBDIVISION_ISO_CODE:
                        enrichData(geoData, GeoIPField.MOST_SPECIFIED_SUBDIVISION_ISO_CODE.getFieldName(), subdivision.getIsoCode());
                        break;
                    case MOST_SPECIFIED_SUBDIVISION_CONFIDENCE:
                        if (isEnterpriseDatabase)
                            enrichData(geoData, GeoIPField.MOST_SPECIFIED_SUBDIVISION_CONFIDENCE.getFieldName(), subdivision.getConfidence());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, GeoIPField.MOST_SPECIFIED_SUBDIVISION_NAME.getFieldName(), subdivision.getName());
            enrichData(geoData, GeoIPField.MOST_SPECIFIED_SUBDIVISION_ISO_CODE.getFieldName(), subdivision.getIsoCode());
            if (isEnterpriseDatabase)
                enrichData(geoData, GeoIPField.MOST_SPECIFIED_SUBDIVISION_CONFIDENCE.getFieldName(), subdivision.getConfidence());
        }
    }

    default void extractLeastSpecifiedSubdivisionFields(final Subdivision subdivision,
                                          final Map<String, Object> geoData,
                                          final List<GeoIPField> fields,
                                          final boolean isEnterpriseDatabase) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case LEAST_SPECIFIED_SUBDIVISION_NAME:
                        enrichData(geoData, GeoIPField.LEAST_SPECIFIED_SUBDIVISION_NAME.getFieldName(), subdivision.getName());
                        break;
                    case LEAST_SPECIFIED_SUBDIVISION_ISO_CODE:
                        enrichData(geoData, GeoIPField.LEAST_SPECIFIED_SUBDIVISION_ISO_CODE.getFieldName(), subdivision.getIsoCode());
                        break;
                    case LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE:
                        if (isEnterpriseDatabase)
                            enrichData(geoData, GeoIPField.LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE.getFieldName(), subdivision.getConfidence());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, GeoIPField.LEAST_SPECIFIED_SUBDIVISION_NAME.getFieldName(), subdivision.getName());
            enrichData(geoData, GeoIPField.LEAST_SPECIFIED_SUBDIVISION_ISO_CODE.getFieldName(), subdivision.getIsoCode());
            if (isEnterpriseDatabase)
                enrichData(geoData, GeoIPField.LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE.getFieldName(), subdivision.getConfidence());
        }
    }

    default void extractAsnFields(final AsnResponse asnResponse,
                                  final Map<String, Object> geoData,
                                  final List<GeoIPField> fields) {
        if (!fields.isEmpty()) {
            for (final GeoIPField field : fields) {
                switch (field) {
                    case ASN:
                        enrichData(geoData, GeoIPField.ASN.getFieldName(), asnResponse.getAutonomousSystemNumber());
                        break;
                    case ASN_ORGANIZATION:
                        enrichData(geoData, GeoIPField.ASN_ORGANIZATION.getFieldName(), asnResponse.getAutonomousSystemOrganization());
                        break;
                    case NETWORK:
                        enrichData(geoData, GeoIPField.NETWORK.getFieldName(), asnResponse.getNetwork().toString());
                        break;
                    case IP:
                        enrichData(geoData, GeoIPField.IP.getFieldName(), asnResponse.getIpAddress());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, GeoIPField.ASN.getFieldName(), asnResponse.getAutonomousSystemNumber());
            enrichData(geoData, GeoIPField.ASN_ORGANIZATION.getFieldName(), asnResponse.getAutonomousSystemOrganization());
            enrichData(geoData, GeoIPField.NETWORK.getFieldName(), asnResponse.getNetwork().toString());
            enrichData(geoData, GeoIPField.IP.getFieldName(), asnResponse.getIpAddress());
        }
    }


}
