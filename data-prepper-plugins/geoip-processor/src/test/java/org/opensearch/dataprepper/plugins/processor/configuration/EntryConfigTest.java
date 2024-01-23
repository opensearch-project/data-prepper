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
    public static final String SOURCE_VALUE = "source";
    public static final String TARGET_VALUE = "target";
    public static final List<String> FIELDS_VALUE = List.of("city", "country");
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
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "source", SOURCE_VALUE);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "target", TARGET_VALUE);
        ReflectivelySetField.setField(EntryConfig.class, entryConfig, "fields", FIELDS_VALUE);

        assertThat(entryConfig.getSource(), equalTo(SOURCE_VALUE));
        assertThat(entryConfig.getTarget(), equalTo(TARGET_VALUE));
        assertThat(entryConfig.getFields(), equalTo(FIELDS_VALUE));
    }
}
