/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FieldConflictStrategyTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ParameterizedTest
    @CsvSource({
            "SKIP,skip",
            "OVERWRITE,overwrite",
            "MERGE_PRESERVE_EXISTING_KEYS,merge_preserve_existing_keys",
            "MERGE_OVERWRITE_EXISTING_KEYS,merge_overwrite_existing_keys"
    })
    void getOptionName_returnsConfiguredName(final FieldConflictStrategy strategy, final String expectedName) {
        assertThat(strategy.getOptionName(), is(expectedName));
    }

    @ParameterizedTest
    @CsvSource({
            "SKIP,false",
            "OVERWRITE,true",
            "MERGE_PRESERVE_EXISTING_KEYS,false",
            "MERGE_OVERWRITE_EXISTING_KEYS,true"
    })
    void isOverwrite_reflectsStrategy(final FieldConflictStrategy strategy, final boolean expected) {
        assertThat(strategy.isOverwrite(), is(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "skip,SKIP",
            "overwrite,OVERWRITE",
            "merge_preserve_existing_keys,MERGE_PRESERVE_EXISTING_KEYS",
            "merge_overwrite_existing_keys,MERGE_OVERWRITE_EXISTING_KEYS"
    })
    void fromOptionName_resolvesByName(final String optionName, final FieldConflictStrategy expected) {
        assertThat(FieldConflictStrategy.fromOptionName(optionName), is(expected));
    }

    @Test
    void fromOptionName_isCaseInsensitive() {
        assertThat(FieldConflictStrategy.fromOptionName("SKIP"), is(FieldConflictStrategy.SKIP));
        assertThat(FieldConflictStrategy.fromOptionName("Merge_Overwrite_Existing_Keys"),
                is(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS));
    }

    @Test
    void fromOptionName_null_returnsNull() {
        assertThat(FieldConflictStrategy.fromOptionName(null), is(nullValue()));
    }

    @Test
    void fromOptionName_invalid_throws() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> FieldConflictStrategy.fromOptionName("not_a_strategy"));
        assertThat(exception.getMessage().contains("not_a_strategy"), is(true));
    }

    @ParameterizedTest
    @EnumSource(FieldConflictStrategy.class)
    void jackson_roundTrip(final FieldConflictStrategy strategy) throws Exception {
        final String json = OBJECT_MAPPER.writeValueAsString(strategy);
        assertThat(OBJECT_MAPPER.readValue(json, FieldConflictStrategy.class), is(strategy));
    }

    @Test
    void jacksonDeserialize_invalidJsonString_throws() {
        assertThrows(Exception.class,
                () -> OBJECT_MAPPER.readValue("\"not_a_strategy\"", FieldConflictStrategy.class));
    }
}
