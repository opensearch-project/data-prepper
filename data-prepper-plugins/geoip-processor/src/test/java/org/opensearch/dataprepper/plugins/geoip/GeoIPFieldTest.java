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
import java.util.EnumSet;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
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

    @ParameterizedTest
    @EnumSource(GeoIPField.class)
    void getGeoIPDatabasesForFields_returns_non_empty_for_all_fields(final GeoIPField field) {
        final Collection<GeoIPDatabase> geoIPDatabases = field.getGeoIPDatabases();
        assertThat(geoIPDatabases, notNullValue());
        assertThat(geoIPDatabases, not(empty()));
    }

    @ParameterizedTest
    @ArgumentsSource(SampleOfKnownDatabasesForField.class)
    void getGeoIPDatabasesForFields_returns_expected_results(final GeoIPField field, final Collection<GeoIPDatabase> expectedDatabases) {
        assertThat(field.getGeoIPDatabases(),
            equalTo(expectedDatabases));
    }

    static class SampleOfKnownDatabasesForField implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(CONTINENT_CODE, EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.COUNTRY, GeoIPDatabase.ENTERPRISE)),
                    arguments(COUNTRY_CONFIDENCE, EnumSet.of(GeoIPDatabase.ENTERPRISE)),
                    arguments(LOCATION, EnumSet.of(GeoIPDatabase.CITY, GeoIPDatabase.ENTERPRISE)),
                    arguments(ASN_ORGANIZATION, EnumSet.of(GeoIPDatabase.ASN))
            );
        }
    }

    @ParameterizedTest
    @EnumSource(GeoIPField.class)
    void allFields_includes_each_enum_value(final GeoIPField geoIPField) {
        final Collection<GeoIPField> allFields = GeoIPField.allFields();
        assertThat(allFields, notNullValue());
        assertThat(allFields.size(), equalTo(GeoIPField.values().length));
        assertThat(allFields, hasItem(geoIPField));
    }
}