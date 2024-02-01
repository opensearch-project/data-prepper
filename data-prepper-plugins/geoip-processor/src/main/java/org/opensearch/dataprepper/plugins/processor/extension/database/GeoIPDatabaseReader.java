/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.database;

import java.util.Map;

/**
 * Interface for storing and maintaining MaxMind database readers
 */

public interface GeoIPDatabaseReader {
    /**
     * Gets the geo data from the {@link com.maxmind.geoip2.DatabaseReader}
     *
     * @param ipAddress IP Address
     * @return Map of geo field and value pairs from IP address
     *
     * @since 2.7
     */
    Map<String, Object> getGeoData(final String ipAddress);

    /**
     * Gets if the database is expired from metadata or last updated timestamp
     *
     * @return boolean indicating if database is expired
     *
     * @since 2.7
     */
    boolean isDatabaseExpired();

    /**
     * Retains the reader which prevents from closing if it's being used
     *
     * @since 2.7
     */
    void retain();

}
