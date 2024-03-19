/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIPDatabaseReader;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link GeoIPDatabaseReader} useful for a single batch of processing.
 */
class BatchGeoIPDatabaseReader implements GeoIPDatabaseReader {
    private final GeoIPDatabaseReader delegate;
    private Boolean isExpired = null;

    BatchGeoIPDatabaseReader(final GeoIPDatabaseReader delegate) {
        this.delegate = delegate;
    }

    static BatchGeoIPDatabaseReader decorate(final GeoIPDatabaseReader delegate) {
        if(delegate == null)
            return null;
        return new BatchGeoIPDatabaseReader(delegate);
    }

    @Override
    public Map<String, Object> getGeoData(final InetAddress inetAddress, final List<GeoIPField> fields, final Set<GeoIPDatabase> geoIPDatabases) {
        return delegate.getGeoData(inetAddress, fields, geoIPDatabases);
    }

    @Override
    public boolean isExpired() {
        if(isExpired == null) {
            isExpired = delegate.isExpired();
        }
        return isExpired;
    }

    @Override
    public void retain() {
        throw new UnsupportedOperationException("GeoIP processor should not call retain. This is a coding error.");
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
