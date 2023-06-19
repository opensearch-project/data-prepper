/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

/**
 * Interface for Downloading through S3 or URl or from local path
 */
public interface GetGeoData {

    public void switchDatabaseReader();
    public void closeReader();
    public Map<String, Object> getGeoData(InetAddress inetAddress, List<String> attributes);

    default void enrichData(Map<String, Object> geoData,String attributeName, String attributeValue) {
    }

   default void enrichRegionIsoCode(Map<String, Object> geoData, String countryIso, String subdivisionIso) {
    }

    default void enrichLocationData(Map<String, Object> geoData, Double latitude, Double longitude) {

    }
}
