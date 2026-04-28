/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DiscoveryModeTest {

    @ParameterizedTest
    @ValueSource(strings = {"PERIODIC", "periodic", "Periodic"})
    void fromString_resolves_periodic_case_insensitively(final String value) {
        assertThat(DiscoveryMode.fromString(value), equalTo(DiscoveryMode.PERIODIC));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SINGLE_SCAN", "single_scan", "Single_Scan"})
    void fromString_resolves_single_scan_case_insensitively(final String value) {
        assertThat(DiscoveryMode.fromString(value), equalTo(DiscoveryMode.SINGLE_SCAN));
    }

    @Test
    void fromString_with_unknown_value_throws_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> DiscoveryMode.fromString("ONCE"));
    }
}
