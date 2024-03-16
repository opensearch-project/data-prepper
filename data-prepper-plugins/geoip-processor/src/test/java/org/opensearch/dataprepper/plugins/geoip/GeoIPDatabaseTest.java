/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase.ASN;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase.CITY;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase.COUNTRY;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase.ENTERPRISE;

class GeoIPDatabaseTest {

    @Test
    void selectDatabases_throws_NullPointerException_for_null_databases() {
        assertThrows(NullPointerException.class, () -> GeoIPDatabase.selectDatabases(null));
    }

    @Test
    void selectDatabases_returns_empty_for_empty_databases() {
        assertThat(GeoIPDatabase.selectDatabases(Collections.emptyList()),
                equalTo(Collections.emptySet()));
    }

    @ParameterizedTest
    @EnumSource(GeoIPDatabase.class)
    void selectDatabases_returns_input_for_single_Database(final GeoIPDatabase database) {
        assertThat(GeoIPDatabase.selectDatabases(Collections.singleton(database)),
                equalTo(Collections.singleton(database)));
    }

    @ParameterizedTest
    @ArgumentsSource(KnownSelectionsForDatabases.class)
    void selectDatabases_returns_expected_databases(final Collection<GeoIPDatabase> inputDatabases,
                                                    final Collection<GeoIPDatabase> expectedSelections) {
        assertThat(GeoIPDatabase.selectDatabases(inputDatabases),
                equalTo(expectedSelections));
    }

    @Test
    void selectDatabasesForFields_returns_empty_for_empty_fields() {
        assertThat(GeoIPDatabase.selectDatabasesForFields(Collections.emptyList()),
                equalTo(EnumSet.noneOf(GeoIPDatabase.class)));
    }

    static class KnownSelectionsForDatabases implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(List.of(CITY, ASN), EnumSet.of(CITY, ASN)),
                    arguments(List.of(CITY, ASN, ENTERPRISE), EnumSet.of(CITY, ASN, ENTERPRISE)),
                    arguments(List.of(COUNTRY, ASN), EnumSet.of(COUNTRY, ASN)),
                    arguments(List.of(COUNTRY, ASN, ENTERPRISE), EnumSet.of(COUNTRY, ASN, ENTERPRISE)),
                    arguments(List.of(CITY, COUNTRY), EnumSet.of(CITY)),
                    arguments(List.of(COUNTRY, CITY), EnumSet.of(CITY)),
                    arguments(List.of(COUNTRY, CITY, ASN), EnumSet.of(CITY, ASN))
            );
        }
    }
}