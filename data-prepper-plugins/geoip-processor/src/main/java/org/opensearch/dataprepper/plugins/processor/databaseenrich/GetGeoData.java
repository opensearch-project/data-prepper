/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for Downloading through S3 or URl or from local path
 */
public interface GetGeoData {

    public final String GeoLite2CityDB = "GeoLite2-City.mmdb";
    public final String GeoLite2CountryDB = "GeoLite2-Country.mmdb";
    public final String GeoLite2AsnDB = "GeoLite2-ASN.mmdb";
    public final String GeoIP2EnterpriseDB = "GeoIP2-Enterprise.mmdb";

    void switchDatabaseReader();
    void closeReader();
    Map<String, Object> getGeoData(InetAddress inetAddress, List<String> attributes);

    /**
     * Enrich attributes
     * @param geoData geoData
     * @param attributeName attributeName
     * @param attributeValue attributeValue
     */
    default void enrichData(final Map<String, Object> geoData, final String attributeName, final String attributeValue) {
        if (attributeValue != null) {
            geoData.put(attributeName, attributeValue);
        }
    }

    /**
     * Enrich region iso code
     * @param geoData geoData
     * @param countryIso countryIso
     * @param subdivisionIso subdivisionIso
     */
    default void enrichRegionIsoCode(final Map<String, Object> geoData, final String countryIso, final String subdivisionIso) {
       if (countryIso != null && subdivisionIso != null) {
           enrichData(geoData, "region_iso_code", countryIso + "-" + subdivisionIso);
       }
    }

    /**
     * Enrich Location Data
     * @param geoData geoData
     * @param latitude latitude
     * @param longitude longitude
     */
    default void enrichLocationData(final Map<String, Object> geoData, final Double latitude, final Double longitude) {
        if (latitude != null && longitude != null) {
            Map<String, Object> locationObject = new HashMap<>();
            locationObject.put("lat", latitude);
            locationObject.put("lon", longitude);
            geoData.put("location", locationObject);
        }
    }
}
