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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.ASN_ORGANIZATION;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.CONTINENT_CODE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.COUNTRY_CONFIDENCE;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.LOCATION;

class GeoIPFieldTest {

    @Test
    void test_findByName_should_return_geoip_field_if_valid() {
        final GeoIPField geoIPField = GeoIPField.findByName("city_name");
        assertThat(geoIPField, equalTo(GeoIPField.CITY_NAME));
    }

    @Test
    void test_findByName_should_return_null_if_invalid() {
        final GeoIPField geoIPField = GeoIPField.findByName("coordinates");
        assertThat(geoIPField, equalTo(null));
    }

    @Test
    void getGeoIPDatabasesForFields_throws_if_given_null_list() {
        assertThrows(NullPointerException.class, () -> GeoIPField.getGeoIPDatabasesForFields(null));
    }

    @Test
    void getGeoIPDatabasesForFields_returns_empty_collection_when_given_empty_collection() {
        final Collection<GeoIPDatabase> actualDatabases = GeoIPField.getGeoIPDatabasesForFields(Collections.emptyList());
        assertThat(actualDatabases, notNullValue());
        assertThat(actualDatabases, empty());
    }

    @ParameterizedTest
    @ArgumentsSource(KnownDatabasesForFields.class)
    void getGeoIPDatabasesForFields_returns_expected_results(final Collection<GeoIPField> providedFields, final Collection<GeoIPDatabase> expectedDatabases) {
        final Collection<GeoIPDatabase> actualDatabases = GeoIPField.getGeoIPDatabasesForFields(providedFields);
        assertThat(actualDatabases, notNullValue());
        assertThat(actualDatabases.size(), equalTo(expectedDatabases.size()));
        assertThat(actualDatabases, equalTo(expectedDatabases));
    }

    static class KnownDatabasesForFields implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(List.of(CONTINENT_CODE), EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(COUNTRY_CONFIDENCE), EnumSet.of(GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(LOCATION), EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(LOCATION, CONTINENT_CODE), EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(ASN_ORGANIZATION), EnumSet.of(GeoIPDatabase.ASN)),
                    arguments(List.of(ASN_ORGANIZATION, COUNTRY_CONFIDENCE), EnumSet.of(GeoIPDatabase.ASN, GeoIPDatabase.ENTERPRISE)),
                    arguments(List.of(ASN_ORGANIZATION, CONTINENT_CODE), EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE, GeoIPDatabase.ASN))
            );
        }
    }
}