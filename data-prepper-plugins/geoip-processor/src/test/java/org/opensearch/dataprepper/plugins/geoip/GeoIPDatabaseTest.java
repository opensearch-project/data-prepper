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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase.CITY;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase.COUNTRY;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.ASN_ORGANIZATION;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.CITY_NAME;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.CONTINENT_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.COUNTRY_CONFIDENCE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LOCATION;

class GeoIPDatabaseTest {
    @Test
    void selectDatabasesForFields_throws_NullPointerException_for_null_databases() {
        assertThrows(NullPointerException.class, () -> GeoIPDatabase.selectDatabasesForFields(null));
    }

    @Test
    void selectDatabasesForFields_returns_empty_for_empty_fields() {
        assertThat(GeoIPDatabase.selectDatabasesForFields(Collections.emptyList()),
                equalTo(EnumSet.noneOf(GeoIPDatabase.class)));
    }

    @ParameterizedTest
    @EnumSource(GeoIPField.class)
    void selectDatabasesForFields_never_returns_both_CITY_and_COUNTRY_for_any_single_field(final GeoIPField geoIPField) {
        final Collection<GeoIPDatabase> actualSelected = GeoIPDatabase.selectDatabasesForFields(Collections.singleton(geoIPField));
        assertThat(actualSelected, notNullValue());
        assertThat(actualSelected, not(empty()));
        assertThat(actualSelected, not(allOf(hasItem(CITY), hasItem(COUNTRY))));
    }

    @ParameterizedTest
    @ArgumentsSource(KnownSelectedDatabasesForFields.class)
    void selectDatabasesForFields_returns_empty_for_empty_fields(
            final Collection<GeoIPField> providedFields,
            final Collection<GeoIPDatabase> expectedDatabases) {
        assertThat(GeoIPDatabase.selectDatabasesForFields(providedFields),
                equalTo(expectedDatabases));
    }

    static class KnownSelectedDatabasesForFields implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(List.of(CONTINENT_CODE), EnumSet.of(GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(COUNTRY_CONFIDENCE), EnumSet.of(GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(LOCATION), EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(LOCATION, CONTINENT_CODE), EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(ASN_ORGANIZATION), EnumSet.of(GeoIPDatabase.ASN)),
                    arguments(List.of(ASN_ORGANIZATION, COUNTRY_CONFIDENCE), EnumSet.of(GeoIPDatabase.ASN, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(ASN_ORGANIZATION, CONTINENT_CODE), EnumSet.of(COUNTRY, GeoIPDatabase.ENTERPRISE, GeoIPDatabase.ASN)),
                    arguments(List.of(ASN_ORGANIZATION, CITY_NAME), EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE, GeoIPDatabase.ASN))
            );
        }
    }
}