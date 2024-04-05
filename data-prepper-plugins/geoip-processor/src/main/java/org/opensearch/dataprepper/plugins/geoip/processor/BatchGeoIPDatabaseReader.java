/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIPDatabaseReader;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link GeoIPDatabaseReader} useful for a single batch of processing.
 */
class BatchGeoIPDatabaseReader implements GeoIPDatabaseReader {
    private final GeoIPDatabaseReader delegate;
    private final Map<GeoDataInput, Map<String, Object>> geoDataCache = new HashMap<>();
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
    public Map<String, Object> getGeoData(final InetAddress inetAddress, final Collection<GeoIPField> fields, final Collection<GeoIPDatabase> geoIPDatabases) {
        final GeoDataInput geoDataInput = new GeoDataInput(inetAddress, fields, geoIPDatabases);
        return geoDataCache.computeIfAbsent(geoDataInput, unused -> delegate.getGeoData(inetAddress, fields, geoIPDatabases));
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

    private static class GeoDataInput {
        final InetAddress inetAddress;
        final Collection<GeoIPField> fields;
        final Collection<GeoIPDatabase> geoIPDatabases;

        private GeoDataInput(InetAddress inetAddress, Collection<GeoIPField> fields, Collection<GeoIPDatabase> geoIPDatabases) {
            this.inetAddress = inetAddress;
            this.fields = fields;
            this.geoIPDatabases = geoIPDatabases;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final GeoDataInput that = (GeoDataInput) o;
            return Objects.equals(inetAddress, that.inetAddress) && Objects.equals(fields, that.fields) && Objects.equals(geoIPDatabases, that.geoIPDatabases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inetAddress, fields, geoIPDatabases);
        }
    }
}
