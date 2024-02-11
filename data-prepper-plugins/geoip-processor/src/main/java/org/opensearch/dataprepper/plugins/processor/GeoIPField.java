/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public enum GeoIPField {
    CONTINENT_CODE("continent_code", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    CONTINENT_NAME("continent_name", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    COUNTRY_NAME("country_name", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    IS_COUNTRY_IN_EUROPEAN_UNION("is_country_in_european_union", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    COUNTRY_ISO_CODE("country_iso_code", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    COUNTRY_CONFIDENCE("country_confidence", GeoIPDatabase.ENTERPRISE),
    REGISTERED_COUNTRY_NAME("registered_country_name", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    REGISTERED_COUNTRY_ISO_CODE("registered_country_iso_code", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    REPRESENTED_COUNTRY_NAME("represented_country_name", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    REPRESENTED_COUNTRY_ISO_CODE("represented_country_iso_code", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    REPRESENTED_COUNTRY_TYPE("represented_country_type", GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE),
    CITY_NAME("city_name", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    CITY_CONFIDENCE("city_confidence", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    LOCATION("location", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    LATITUDE("latitude", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    LONGITUDE("longitude", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    LOCATION_ACCURACY_RADIUS("location_accuracy_radius", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    METRO_CODE("metro_code", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    TIME_ZONE("time_zone", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    POSTAL_CODE("postal_code", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    POSTAL_CODE_CONFIDENCE("postal_code_confidence", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    MOST_SPECIFIED_SUBDIVISION_NAME("most_specified_subdivision_name", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    MOST_SPECIFIED_SUBDIVISION_ISO_CODE("most_specified_subdivision_iso_code", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    MOST_SPECIFIED_SUBDIVISION_CONFIDENCE("most_specified_subdivision_confidence", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    LEAST_SPECIFIED_SUBDIVISION_NAME("least_specified_subdivision_name", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    LEAST_SPECIFIED_SUBDIVISION_ISO_CODE("least_specified_subdivision_iso_code", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),
    LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE("least_specified_subdivision_confidence", GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE),

    ASN("asn", GeoIPDatabase.ASN),
    ASN_ORGANIZATION("asn_organization", GeoIPDatabase.ASN),
    NETWORK("network", GeoIPDatabase.ASN),
    IP("ip", GeoIPDatabase.ASN);

    private final HashSet<GeoIPDatabase> geoIPDatabases;
    private final String fieldName;

    GeoIPField(final String fieldName, final GeoIPDatabase... geoIPDatabases) {
        this.fieldName = fieldName;
        this.geoIPDatabases = new HashSet<>(Arrays.asList(geoIPDatabases));
    }

    public static GeoIPField findByName(final String name) {
        GeoIPField result = null;
        for (GeoIPField geoIPField : values()) {
            if (geoIPField.getFieldName().equalsIgnoreCase(name)) {
                result = geoIPField;
                break;
            }
        }
        return result;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Set<GeoIPDatabase> getGeoIPDatabases() {
        return geoIPDatabases;
    }
}
