/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public enum GeoIPDatabase {
    CITY,
    COUNTRY(List.of(CITY)),
    ASN;

    private final List<GeoIPDatabase> superSets;

    GeoIPDatabase() {
        this(new ArrayList<>());
    }

    GeoIPDatabase(List<GeoIPDatabase> superSet) {
        this.superSets = superSet;
    }

    public static Set<GeoIPDatabase> process(final Set<GeoIPDatabase> necessaryDatabases, final Set<GeoIPDatabase> availableDatabases) {
        final Set<GeoIPDatabase> finalDatabases = necessaryDatabases;
        for (GeoIPDatabase necessaryDatabase: necessaryDatabases) {
            final List<GeoIPDatabase> superSet = necessaryDatabase.superSets;
            if (!superSet.isEmpty() && necessaryDatabases.contains(superSet.get(0)) && availableDatabases.contains(superSet.get(0))) {
                finalDatabases.remove(necessaryDatabase);
            }
        }

        return finalDatabases;
    }
}
