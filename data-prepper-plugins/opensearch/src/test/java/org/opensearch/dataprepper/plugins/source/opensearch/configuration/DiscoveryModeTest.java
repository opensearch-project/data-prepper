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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DiscoveryModeTest {

    @ParameterizedTest
    @CsvSource({
            "periodic,    PERIODIC",
            "single_scan, SINGLE_SCAN"
    })
    void fromOptionName_returns_expected_enum(final String optionName, final DiscoveryMode expected) {
        assertThat(DiscoveryMode.fromOptionName(optionName), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "PERIODIC,    periodic",
            "SINGLE_SCAN, single_scan"
    })
    void getOptionName_returns_expected_string(final DiscoveryMode mode, final String expected) {
        assertThat(mode.getOptionName(), equalTo(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {"PERIODIC", "Single_Scan", "unknown", ""})
    void fromOptionName_returns_null_for_unknown_value(final String optionName) {
        assertThat(DiscoveryMode.fromOptionName(optionName), nullValue());
    }
}
