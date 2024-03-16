/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

public enum GeoIPDatabase {
    CITY(),
    COUNTRY(CITY),
    ASN(),
    ENTERPRISE();
    private final Collection<GeoIPDatabase> isReplaceableBy;

    GeoIPDatabase(final GeoIPDatabase... isReplaceableBy) {
        this.isReplaceableBy = Arrays.asList(isReplaceableBy);
    }

    public static Collection<GeoIPDatabase> selectDatabasesForFields(final Collection<GeoIPField> geoIPFields) {
        // TODO: With some additional refactoring we can also choose COUNTRY over CITY if no CITY fields are needed.
        final Collection<GeoIPDatabase> geoIPDatabasesForFields = GeoIPField.getGeoIPDatabasesForFields(geoIPFields);
        return GeoIPDatabase.selectDatabases(geoIPDatabasesForFields);
    }

    static Collection<GeoIPDatabase> selectDatabases(final Collection<GeoIPDatabase> databases) {
        if(databases == null)
            throw new NullPointerException("A null databases collection was provided to selectDatabases.");

        final EnumSet<GeoIPDatabase> selectedDatabases = EnumSet.noneOf(GeoIPDatabase.class);
        for (final GeoIPDatabase database : databases) {
            boolean includeDatabase = true;
            for (final GeoIPDatabase isReplacedBy : database.isReplaceableBy) {
                if (databases.contains(isReplacedBy)) {
                    includeDatabase = false;
                    break;
                }
            }

            if(includeDatabase)
                selectedDatabases.add(database);
        }

        return selectedDatabases;
    }
}
