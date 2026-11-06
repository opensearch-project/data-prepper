/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecordTypeTest {

    @Test
    void fromString_returns_string() {
        assertThat(RecordType.fromString("string"), equalTo(RecordType.STRING));
    }

    @Test
    void fromString_returns_event() {
        assertThat(RecordType.fromString("event"), equalTo(RecordType.EVENT));
    }

    @Test
    void fromString_is_case_insensitive() {
        assertThat(RecordType.fromString("STRING"), equalTo(RecordType.STRING));
        assertThat(RecordType.fromString("EVENT"), equalTo(RecordType.EVENT));
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "record", ""})
    void fromString_throws_for_invalid_value(final String value) {
        assertThrows(IllegalArgumentException.class, () -> RecordType.fromString(value));
    }

    @Test
    void fromString_with_null_throws_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> RecordType.fromString(null));
    }

    @Test
    void toString_returns_name() {
        assertThat(RecordType.STRING.toString(), equalTo("string"));
        assertThat(RecordType.EVENT.toString(), equalTo("event"));
    }
}
