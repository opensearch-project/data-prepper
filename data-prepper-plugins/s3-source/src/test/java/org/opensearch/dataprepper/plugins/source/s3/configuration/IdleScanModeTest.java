/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.s3.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IdleScanModeTest {

    @ParameterizedTest
    @EnumSource(IdleScanMode.class)
    void fromString_returns_correct_value_for_all_enum_constants(final IdleScanMode mode) {
        assertThat(IdleScanMode.fromString(mode.toString()), equalTo(mode));
    }

    @ParameterizedTest
    @EnumSource(IdleScanMode.class)
    void fromString_is_case_insensitive(final IdleScanMode mode) {
        assertThat(IdleScanMode.fromString(mode.toString().toUpperCase()), equalTo(mode));
    }

    @ParameterizedTest
    @EnumSource(IdleScanMode.class)
    void toString_returns_non_null(final IdleScanMode mode) {
        assertThat(mode.toString(), notNullValue());
    }

    @Test
    void fromString_throws_for_invalid_value() {
        assertThrows(IllegalArgumentException.class, () -> IdleScanMode.fromString("invalid_mode"));
    }

    @Test
    void fromString_throws_for_null() {
        assertThrows(IllegalArgumentException.class, () -> IdleScanMode.fromString(null));
    }

    @Test
    void idle_optimized_has_correct_string_value() {
        assertThat(IdleScanMode.IDLE_OPTIMIZED.toString(), equalTo("idle_optimized"));
    }

    @Test
    void continuous_has_correct_string_value() {
        assertThat(IdleScanMode.CONTINUOUS.toString(), equalTo("continuous"));
    }
}
