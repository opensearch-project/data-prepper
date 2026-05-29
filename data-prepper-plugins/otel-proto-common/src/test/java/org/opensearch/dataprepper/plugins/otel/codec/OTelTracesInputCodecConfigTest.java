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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class OTelTracesInputCodecConfigTest {

    @Test
    void default_format_is_json() {
        OTelTracesInputCodecConfig config = new OTelTracesInputCodecConfig();
        assertThat(config.getFormat(), equalTo(OTelTracesFormatOption.JSON));
    }

    @Test
    void default_otel_format_is_opensearch() {
        OTelTracesInputCodecConfig config = new OTelTracesInputCodecConfig();
        assertThat(config.getOTelOutputFormat(), equalTo(OTelOutputFormat.OPENSEARCH));
    }

    @Test
    void default_length_prefixed_encoding_is_false() {
        OTelTracesInputCodecConfig config = new OTelTracesInputCodecConfig();
        assertThat(config.getLengthPrefixedEncoding(), equalTo(false));
    }

    @Test
    void isValidFormat_returns_true_with_default() {
        OTelTracesInputCodecConfig config = new OTelTracesInputCodecConfig();
        assertThat(config.isValidFormat(), equalTo(true));
    }
}
