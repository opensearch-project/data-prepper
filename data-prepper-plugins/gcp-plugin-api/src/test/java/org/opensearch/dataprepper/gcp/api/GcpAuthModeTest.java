/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class GcpAuthModeTest {
    @ParameterizedTest
    @EnumSource(GcpAuthMode.class)
    void fromOptionValue_returns_mode_for_its_name(final GcpAuthMode mode) {
        assertThat(GcpAuthMode.fromOptionValue(mode.getOptionValue()), equalTo(mode));
    }

    @Test
    void getOptionValue_returns_yaml_names() {
        assertThat(GcpAuthMode.ADC.getOptionValue(), equalTo("adc"));
        assertThat(GcpAuthMode.SERVICE_ACCOUNT_KEY.getOptionValue(), equalTo("service_account_key"));
    }

    @Test
    void fromOptionValue_with_unknown_name_returns_null() {
        assertThat(GcpAuthMode.fromOptionValue("oauth"), nullValue());
    }

    @Test
    void jackson_round_trips_mode_by_option_value() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        assertThat(objectMapper.writeValueAsString(GcpAuthMode.SERVICE_ACCOUNT_KEY), equalTo("\"service_account_key\""));
        assertThat(objectMapper.readValue("\"adc\"", GcpAuthMode.class), equalTo(GcpAuthMode.ADC));
    }
}
