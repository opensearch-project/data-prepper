/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HecResponseCodeTest {

    @Test
    void success_has_code_0() {
        assertThat(HecResponseCode.SUCCESS.getCode(), equalTo(0));
        assertThat(HecResponseCode.SUCCESS.getText(), equalTo("Success"));
    }

    @Test
    void server_busy_has_code_9() {
        assertThat(HecResponseCode.SERVER_BUSY.getCode(), equalTo(9));
    }

    @Test
    void event_field_required_has_code_12() {
        assertThat(HecResponseCode.EVENT_FIELD_REQUIRED.getCode(), equalTo(12));
    }

    @Test
    void hec_healthy_has_code_17() {
        assertThat(HecResponseCode.HEC_HEALTHY.getCode(), equalTo(17));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18})
    void fromCode_returns_correct_enum_for_valid_codes(final int code) {
        assertThat(HecResponseCode.fromCode(code), notNullValue());
        assertThat(HecResponseCode.fromCode(code).getCode(), equalTo(code));
    }

    @Test
    void fromCode_throws_for_unknown_code() {
        assertThrows(IllegalArgumentException.class, () -> HecResponseCode.fromCode(999));
    }
}
