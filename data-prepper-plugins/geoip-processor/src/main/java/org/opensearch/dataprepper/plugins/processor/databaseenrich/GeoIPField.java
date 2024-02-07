/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public enum GeoIPField {
    CONTINENT_CODE("CONTINENT_CODE", GeoIPDatabase.COUNTRY),
    CONTINENT_NAME("CONTINENT_NAME", GeoIPDatabase.COUNTRY),
    COUNTRY_NAME("COUNTRY_NAME", GeoIPDatabase.COUNTRY),
    IS_COUNTRY_IN_EUROPEAN_UNION("IS_COUNTRY_IN_EUROPEAN_UNION", GeoIPDatabase.COUNTRY),
    COUNTRY_ISO_CODE("COUNTRY_ISO_CODE", GeoIPDatabase.COUNTRY),
    CITY_NAME("CITY_NAME", GeoIPDatabase.CITY),
    LOCATION("LOCATION", GeoIPDatabase.CITY),
    LATITUDE("LATITUDE", GeoIPDatabase.CITY),
    LONGITUDE("LONGITUDE", GeoIPDatabase.CITY),
    METRO_CODE("METRO_CODE", GeoIPDatabase.CITY),
    TIME_ZONE("TIME_ZONE", GeoIPDatabase.CITY),
    POSTAL_CODE("POSTAL_CODE", GeoIPDatabase.CITY),
    MOST_SPECIFIED_SUBDIVISION_NAME("MOST_SPECIFIED_SUBDIVISION_NAME", GeoIPDatabase.CITY),
    MOST_SPECIFIED_SUBDIVISION_ISO_CODE("MOST_SPECIFIED_SUBDIVISION_ISO_CODE", GeoIPDatabase.CITY),

    ASN("ASN", GeoIPDatabase.ASN),
    ASN_ORGANIZATION("ASN_ORGANIZATION", GeoIPDatabase.ASN),
    NETWORK("NETWORK", GeoIPDatabase.ASN);

    private final HashSet<GeoIPDatabase> geoIPDatabases;
    private final String fieldName;

    GeoIPField(final String fieldName, final GeoIPDatabase... geoIPDatabases) {
        this.fieldName = fieldName;
        this.geoIPDatabases = new HashSet<>(Arrays.asList(geoIPDatabases));
    }

    private static final Map<String, Set<GeoIPDatabase>> FIELDS_MAP = Arrays.stream(GeoIPField.values())
            .collect(Collectors.toMap(
                    value -> value.fieldName,
                    value -> value.geoIPDatabases
            ));

    public static Optional<Set<GeoIPDatabase>> getGeoLite2Databases(final String fieldName) {
        final String upperCaseFieldName = fieldName.toUpperCase();
        if (FIELDS_MAP.containsKey(upperCaseFieldName))
            return Optional.of(FIELDS_MAP.get(upperCaseFieldName));
        else
            return Optional.empty();
    }

}
