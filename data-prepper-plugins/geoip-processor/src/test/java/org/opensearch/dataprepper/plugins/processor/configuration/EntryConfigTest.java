/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.plugins.processor.GeoIPField;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.dataprepper.plugins.processor.configuration.EntryConfig.DEFAULT_TARGET;

class EntryConfigTest {

    private EntryConfig createObjectUnderTest() {
        return new EntryConfig();
    }

    @Test
    void testDefaultConfig() {
        final EntryConfig entryConfig = createObjectUnderTest();

        assertThat(entryConfig.getSource(), is(nullValue()));
        assertThat(entryConfig.getTarget(), equalTo(DEFAULT_TARGET));
        assertThat(entryConfig.getFields(), is(Collections.emptyList()));
    }

    @Test
    void testCustomConfig() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();

        final String sourceValue = "source";
        final String targetValue = "target";
        final List<GeoIPField> fieldsValue = List.of(GeoIPField.CITY_NAME, GeoIPField.CONTINENT_CODE);

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "source", sourceValue);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "target", targetValue);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "geoIPFields", fieldsValue);

        assertThat(entryConfig.getSource(), equalTo(sourceValue));
        assertThat(entryConfig.getTarget(), equalTo(targetValue));
        assertThat(entryConfig.getFields(), equalTo(fieldsValue));
    }

    @Test
    void test_getFields_with_include_fields_should_return_correct_geoip_fields() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();
        final List<String> includeFields = List.of("city_name", "continent_code");
        final List<GeoIPField> fieldsValue = List.of(GeoIPField.CITY_NAME, GeoIPField.CONTINENT_CODE);

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "includeFields", includeFields);

        assertThat(entryConfig.getFields().size(), equalTo(fieldsValue.size()));
        assertThat(entryConfig.getFields(), containsInAnyOrder(fieldsValue.toArray()));
    }

    @Test
    void test_getFields_with_exclude_keys_should_return_correct_geoip_fields() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();
        final List<String> excludeFields = List.of("city_name", "continent_code", "continent_name", "is_country_in_european_union",
                "asn", "asn_organization", "network", "ip");
        final List<GeoIPField> fieldsValue = List.of(GeoIPField.LATITUDE,
                GeoIPField.REPRESENTED_COUNTRY_ISO_CODE, GeoIPField.LONGITUDE, GeoIPField.COUNTRY_NAME,
                GeoIPField.COUNTRY_ISO_CODE, GeoIPField.REGISTERED_COUNTRY_ISO_CODE, GeoIPField.REGISTERED_COUNTRY_NAME,
                GeoIPField.COUNTRY_CONFIDENCE, GeoIPField.REPRESENTED_COUNTRY_TYPE, GeoIPField.REPRESENTED_COUNTRY_NAME,
                GeoIPField.CITY_CONFIDENCE, GeoIPField.LOCATION, GeoIPField.LOCATION_ACCURACY_RADIUS, GeoIPField.POSTAL_CODE,
                GeoIPField.METRO_CODE, GeoIPField.TIME_ZONE, GeoIPField.POSTAL_CODE_CONFIDENCE, GeoIPField.MOST_SPECIFIED_SUBDIVISION_NAME,
                GeoIPField.MOST_SPECIFIED_SUBDIVISION_CONFIDENCE, GeoIPField.MOST_SPECIFIED_SUBDIVISION_ISO_CODE,
                GeoIPField.LEAST_SPECIFIED_SUBDIVISION_CONFIDENCE, GeoIPField.LEAST_SPECIFIED_SUBDIVISION_ISO_CODE,
                GeoIPField.LEAST_SPECIFIED_SUBDIVISION_NAME);

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "excludeFields", excludeFields);

        assertThat(entryConfig.getFields().size(), equalTo(fieldsValue.size()));
        assertThat(entryConfig.getFields(), containsInAnyOrder(fieldsValue.toArray()));
    }

    @Test
    void test_areFieldsValid_should_return_true_if_only_include_fields_is_configured() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();
        final List<String> includeFields = List.of("city_name", "continent_code");

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "includeFields", includeFields);

        assertThat(entryConfig.areFieldsValid(), equalTo(true));
    }

    @Test
    void test_areFieldsValid_should_return_true_if_only_exclude_fields_is_configured() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();
        final List<String> excludeFields = List.of("city_name", "continent_code");

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "excludeFields", excludeFields);

        assertThat(entryConfig.areFieldsValid(), equalTo(true));
    }

    @Test
    void test_areFieldsValid_should_return_false_if_both_include_and_exclude_fields_are_configured() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();
        final List<String> includeFields = List.of("city_name", "continent_code");
        final List<String> excludeFields = List.of("asn");

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "includeFields", includeFields);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "excludeFields", excludeFields);

        assertThat(entryConfig.areFieldsValid(), equalTo(false));
    }

    @Test
    void test_areFieldsValid_should_return_false_if_both_include_and_exclude_fields_are_empty() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();
        final List<String> includeFields = Collections.emptyList();
        final List<String> excludeFields = Collections.emptyList();

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "includeFields", includeFields);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "excludeFields", excludeFields);

        assertThat(entryConfig.areFieldsValid(), equalTo(false));
    }
}
