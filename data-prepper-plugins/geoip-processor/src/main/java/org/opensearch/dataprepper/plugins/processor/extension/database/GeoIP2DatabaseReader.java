/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.database;

import java.util.Map;

public class GeoIP2DatabaseReader implements GeoIPDatabaseReader, AutoCloseable {
    @Override
    public Map<String, Object> getGeoData(String ipAddress) {
        return null;
    }

    @Override
    public boolean isDatabaseExpired() {
        return false;
    }

    @Override
    public void retain() {

    }

    @Override
    public void close() {

    }
}
