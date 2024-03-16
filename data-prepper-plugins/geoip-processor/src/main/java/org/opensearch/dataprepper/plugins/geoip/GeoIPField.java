/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * GeoIP fields and their corresponding databases.
 * The fields are available at
 * <a href="https://dev.maxmind.com/geoip/docs/databases/city-and-country">GeoIP2 and GeoLite City and Country Databases</a>
 *
 */
public enum GeoIPField {
    CONTINENT_CODE("continent_code", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
    CONTINENT_NAME("continent_name", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
    COUNTRY_NAME("country_name", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
    IS_COUNTRY_IN_EUROPEAN_UNION("is_country_in_european_union", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
    COUNTRY_ISO_CODE("country_iso_code", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
    COUNTRY_CONFIDENCE("country_confidence", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    REGISTERED_COUNTRY_NAME("registered_country_name", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    REGISTERED_COUNTRY_ISO_CODE("registered_country_iso_code", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    REPRESENTED_COUNTRY_NAME("represented_country_name", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    REPRESENTED_COUNTRY_ISO_CODE("represented_country_iso_code", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    REPRESENTED_COUNTRY_TYPE("represented_country_type", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    CITY_NAME("city_name", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    CITY_CONFIDENCE("city_confidence", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    LOCATION("location", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    LATITUDE("latitude", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    LONGITUDE("longitude", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    LOCATION_ACCURACY_RADIUS("location_accuracy_radius", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    METRO_CODE("metro_code", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    TIME_ZONE("time_zone", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    POSTAL_CODE("postal_code", EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
    POSTAL_CODE_CONFIDENCE("postal_code_confidence", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    MOST_SPECIFIED_SUBDIVISION_NAME("most_specified_subdivision_name", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    MOST_SPECIFIED_SUBDIVISION_ISO_CODE("most_specified_subdivision_iso_code", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    MOST_SPECIFIED_SUBDIVISION_CONFIDENCE("most_specified_subdivision_confidence", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    LEAST_SPECIFIED_SUBDIVISION_NAME("least_specified_subdivision_name", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    LEAST_SPECIFIED_SUBDIVISION_ISO_CODE("least_specified_subdivision_iso_code", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE("least_specified_subdivision_confidence", EnumSet.of(GeoIPDatabase.ENTERPRISE)),
    ASN("asn", EnumSet.of(GeoIPDatabase.ASN)),
    ASN_ORGANIZATION("asn_organization", EnumSet.of(GeoIPDatabase.ASN)),
    NETWORK("network", EnumSet.of(GeoIPDatabase.ASN)),
    IP("ip", EnumSet.of(GeoIPDatabase.ASN));

    private final Set<GeoIPDatabase> geoIPDatabases;
    private final String fieldName;

    GeoIPField(final String fieldName, final EnumSet<GeoIPDatabase> geoIPDatabases) {
        this.fieldName = fieldName;
        this.geoIPDatabases = geoIPDatabases;
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

    Collection<GeoIPDatabase> getGeoIPDatabases() {
        return geoIPDatabases;
    }
}
