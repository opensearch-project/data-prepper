/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class OTelTracesFormatOptionTest {

    @ParameterizedTest
    @EnumSource(OTelTracesFormatOption.class)
    void fromFormatName_returns_correct_value(OTelTracesFormatOption option) {
        assertThat(OTelTracesFormatOption.fromFormatName(option.getFormatName()), equalTo(option));
    }

    @Test
    void fromFormatName_returns_null_for_unknown() {
        assertThat(OTelTracesFormatOption.fromFormatName("unknown"), nullValue());
    }

    @Test
    void json_has_correct_name() {
        assertThat(OTelTracesFormatOption.JSON.getFormatName(), equalTo("json"));
    }

    @Test
    void protobuf_has_correct_name() {
        assertThat(OTelTracesFormatOption.PROTOBUF.getFormatName(), equalTo("protobuf"));
    }
}
