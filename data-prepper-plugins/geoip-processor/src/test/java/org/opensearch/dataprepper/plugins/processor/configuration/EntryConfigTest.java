/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.dataprepper.plugins.processor.configuration.EntryConfig.DEFAULT_TARGET;

class EntryConfigTest {
    private EntryConfig entryConfig;

    @BeforeEach
    void setUp() {
        entryConfig = new EntryConfig();
    }

    @Test
    void testDefaultConfig() {
        assertThat(entryConfig.getSource(), is(nullValue()));
        assertThat(entryConfig.getTarget(), equalTo(DEFAULT_TARGET));
        assertThat(entryConfig.getFields(), is(nullValue()));
    }

    @Test
    void testCustomConfig() throws NoSuchFieldException, IllegalAccessException {
        final String sourceValue = "source";
        final String targetValue = "target";
        final List<String> fieldsValue = List.of("city", "country");

        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "source", sourceValue);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "target", targetValue);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "fields", fieldsValue);

        assertThat(entryConfig.getSource(), equalTo(sourceValue));
        assertThat(entryConfig.getTarget(), equalTo(targetValue));
        assertThat(entryConfig.getFields(), equalTo(fieldsValue));
    }
}
