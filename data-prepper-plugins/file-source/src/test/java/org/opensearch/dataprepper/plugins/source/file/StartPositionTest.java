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

class StartPositionTest {

    @Test
    void fromString_returns_beginning_for_beginning() {
        assertThat(StartPosition.fromString("beginning"), equalTo(StartPosition.BEGINNING));
    }

    @Test
    void fromString_returns_end_for_end() {
        assertThat(StartPosition.fromString("end"), equalTo(StartPosition.END));
    }

    @Test
    void fromString_is_case_insensitive() {
        assertThat(StartPosition.fromString("BEGINNING"), equalTo(StartPosition.BEGINNING));
        assertThat(StartPosition.fromString("END"), equalTo(StartPosition.END));
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "start", "middle", ""})
    void fromString_throws_for_invalid_value(final String value) {
        assertThrows(IllegalArgumentException.class, () -> StartPosition.fromString(value));
    }

    @Test
    void toString_returns_name() {
        assertThat(StartPosition.BEGINNING.toString(), equalTo("beginning"));
        assertThat(StartPosition.END.toString(), equalTo("end"));
    }

    @Test
    void fromString_with_null_throws_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> StartPosition.fromString(null));
    }
}
