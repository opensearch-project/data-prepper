/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip;

import java.util.Collection;
import java.util.EnumSet;

public enum GeoIPDatabase {
    COUNTRY,
    CITY,
    ASN,
    ENTERPRISE;

    /**
     * Selects the databases needed for the given GeoIP fields. This will choose the
     * as many databases as needed to extract the requested fields. But, it will also
     * reduce the databases to avoid redundant calls.
     *
     * @param geoIPFields The necessary GeoIP fields
     * @return The GeoIPDatabases needed to extract all fields.
     */
    public static Collection<GeoIPDatabase> selectDatabasesForFields(final Collection<GeoIPField> geoIPFields) {
        if(geoIPFields == null)
            throw new NullPointerException("The geoIPFields parameter must be non-null.");

        final EnumSet<GeoIPDatabase> selectedDatabases = EnumSet.noneOf(GeoIPDatabase.class);
        for (final GeoIPField geoIPField : geoIPFields) {
            selectedDatabases.addAll(leastCommonDatabase(geoIPField.getGeoIPDatabases()));
        }

        if(selectedDatabases.contains(CITY)) {
            selectedDatabases.remove(COUNTRY);
        }

        return selectedDatabases;
    }

    /**
     * Gets the fewest number of databases needed for the databases provided. This
     * generally expects that the input covers a single field.
     *
     * @param databases All the databases that have the data.
     * @return The least common set of databases
     */
    private static Collection<GeoIPDatabase> leastCommonDatabase(final Collection<GeoIPDatabase> databases) {
        if(databases == null)
            throw new NullPointerException("A null databases collection was provided to selectDatabases.");

        final EnumSet<GeoIPDatabase> selectedDatabases = EnumSet.copyOf(databases);
        if(selectedDatabases.contains(CITY) && selectedDatabases.contains(COUNTRY))
            selectedDatabases.remove(CITY);

        return selectedDatabases;
    }
}
