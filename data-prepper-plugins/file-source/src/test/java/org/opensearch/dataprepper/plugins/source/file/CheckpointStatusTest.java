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

class CheckpointStatusTest {

    @Test
    void fromString_returns_active() {
        assertThat(CheckpointStatus.fromString("ACTIVE"), equalTo(CheckpointStatus.ACTIVE));
    }

    @Test
    void fromString_returns_completed() {
        assertThat(CheckpointStatus.fromString("COMPLETED"), equalTo(CheckpointStatus.COMPLETED));
    }

    @Test
    void fromString_is_case_insensitive() {
        assertThat(CheckpointStatus.fromString("active"), equalTo(CheckpointStatus.ACTIVE));
        assertThat(CheckpointStatus.fromString("completed"), equalTo(CheckpointStatus.COMPLETED));
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "pending", ""})
    void fromString_throws_for_invalid_value(final String value) {
        assertThrows(IllegalArgumentException.class, () -> CheckpointStatus.fromString(value));
    }

    @Test
    void getValue_returns_value() {
        assertThat(CheckpointStatus.ACTIVE.getValue(), equalTo("ACTIVE"));
        assertThat(CheckpointStatus.COMPLETED.getValue(), equalTo("COMPLETED"));
    }
}
