/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class MultilineInputCodecConfigTest {

    @Test
    void defaults_are_correct() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();

        assertThat(config.getNegate(), equalTo(false));
        assertThat(config.getWhat(), equalTo(MultilineWhat.PREVIOUS));
        assertThat(config.getMaxLines(), equalTo(MultilineInputCodecConfig.DEFAULT_MAX_LINES));
        assertThat(config.getMaxLength(), equalTo(MultilineInputCodecConfig.DEFAULT_MAX_LENGTH));
        assertThat(config.getLineSeparator(), equalTo(MultilineInputCodecConfig.DEFAULT_LINE_SEPARATOR));
        assertThat(config.getMatch(), equalTo(null));
    }

    @Test
    void getMatch_returns_configured_value() throws Exception {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        setField(config, "match", "^\\d{4}");
        assertThat(config.getMatch(), equalTo("^\\d{4}"));
    }

    @Test
    void isValidPattern_returns_true_for_valid_regex() throws Exception {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        setField(config, "match", "^\\d{4}-\\d{2}-\\d{2}");
        assertThat(config.isValidPattern(), equalTo(true));
    }

    @Test
    void isValidPattern_returns_false_for_invalid_regex() throws Exception {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        setField(config, "match", "[invalid(");
        assertThat(config.isValidPattern(), equalTo(false));
    }

    @Test
    void isValidPattern_returns_false_for_null_match() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        assertThat(config.isValidPattern(), equalTo(false));
    }

    @Test
    void isValidPattern_returns_false_for_empty_match() throws Exception {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        setField(config, "match", "");
        assertThat(config.isValidPattern(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 100, 1000})
    void getMaxLines_returns_configured_value(final int maxLines) throws Exception {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        setField(config, "maxLines", maxLines);
        assertThat(config.getMaxLines(), equalTo(maxLines));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5000, 50000})
    void getMaxLength_returns_configured_value(final int maxLength) throws Exception {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        setField(config, "maxLength", maxLength);
        assertThat(config.getMaxLength(), equalTo(maxLength));
    }

    private void setField(final Object object, final String fieldName, final Object value) throws Exception {
        final Field field = object.getClass().getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(object, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
