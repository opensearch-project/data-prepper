/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.dataprepper.plugins.geoip.processor.EntryConfig.DEFAULT_TARGET;

class EntryConfigTest {

    private EntryConfig createObjectUnderTest() {
        return new EntryConfig();
    }

    @Test
    void testDefaultConfig() {
        final EntryConfig entryConfig = createObjectUnderTest();

        assertThat(entryConfig.getSource(), is(nullValue()));
        assertThat(entryConfig.getTarget(), equalTo(DEFAULT_TARGET));
        assertThat(entryConfig.getIncludeFields(), equalTo(null));
        assertThat(entryConfig.getExcludeFields(), equalTo(null));
    }

    @Test
    void testCustomConfig() throws NoSuchFieldException, IllegalAccessException {
        final EntryConfig entryConfig = createObjectUnderTest();

        final String sourceValue = "source";
        final String targetValue = "target";
        final List<String> includeFieldsValue = List.of("city_name");
        final List<String> excludeFieldsValue = List.of("asn");

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "source", sourceValue);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "target", targetValue);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "includeFields", includeFieldsValue);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "excludeFields", excludeFieldsValue);

        assertThat(entryConfig.getSource(), equalTo(sourceValue));
        assertThat(entryConfig.getTarget(), equalTo(targetValue));
        assertThat(entryConfig.getIncludeFields(), equalTo(includeFieldsValue));
        assertThat(entryConfig.getExcludeFields(), equalTo(excludeFieldsValue));
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
    void test_areFieldsValid_should_return_false_if_both_include_and_exclude_fields_are_not_configured() {
        final EntryConfig entryConfig = createObjectUnderTest();

        assertThat(entryConfig.areFieldsValid(), equalTo(false));
    }
}
