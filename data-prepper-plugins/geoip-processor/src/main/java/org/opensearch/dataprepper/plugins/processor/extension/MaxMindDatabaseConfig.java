/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSourceOptions;
import org.opensearch.dataprepper.plugins.processor.utils.DatabaseSourceIdentification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaxMindDatabaseConfig {
    static final String DEFAULT_CITY_ENDPOINT = "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-city/manifest.json";
    static final String DEFAULT_COUNTRY_ENDPOINT = "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-country/manifest.json";
    static final String DEFAULT_ASN_ENDPOINT = "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-asn/manifest.json";
    public static final String GEOLITE2_COUNTRY = "geolite2-country";
    public static final String GEOLITE2_CITY = "geolite2-city";
    public static final String GEOLITE2_ASN = "geolite2-asn";
    public static final String GEOIP2_ENTERPRISE = "geoip2-enterprise";
    private Map<String, String> databases = null;
    @JsonProperty("city")
    private String cityDatabase;

    @JsonProperty("country")
    private String countryDatabase;

    @JsonProperty("asn")
    private String asnDatabase;

    @JsonProperty("enterprise")
    private String enterpriseDatabase;

    @AssertTrue(message = "MaxMind GeoLite2 databases cannot be used along with enterprise database.")
    public boolean isDatabasesValid() {
        return enterpriseDatabase == null || (cityDatabase == null && countryDatabase == null && asnDatabase == null);
    }

    @AssertTrue(message = "database_paths should be S3 URI or HTTP endpoint or local directory")
    public boolean isPathsValid() {
        final List<String> databasePaths = new ArrayList<>(getDatabasePaths().values());

        final DBSourceOptions dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(databasePaths);
        return dbSourceOptions != null;
    }

    public Map<String, String> getDatabasePaths() {
        if (databases == null) {
            databases = new HashMap<>();
            if (countryDatabase == null && cityDatabase == null && asnDatabase == null && enterpriseDatabase == null) {
                databases.put(GEOLITE2_COUNTRY, DEFAULT_COUNTRY_ENDPOINT);
                databases.put(GEOLITE2_CITY, DEFAULT_CITY_ENDPOINT);
                databases.put(GEOLITE2_ASN, DEFAULT_ASN_ENDPOINT);
            } else {
                if (countryDatabase != null) {
                    databases.put(GEOLITE2_COUNTRY, countryDatabase);
                }
                if (cityDatabase != null) {
                    databases.put(GEOLITE2_CITY, cityDatabase);
                }
                if (asnDatabase != null) {
                    databases.put(GEOLITE2_ASN, asnDatabase);
                }
                if (enterpriseDatabase != null) {
                    databases.put(GEOIP2_ENTERPRISE, enterpriseDatabase);
                }
            }
        }
        return databases;
    }
}
