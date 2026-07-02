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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class MultilineInputCodecConfigTest {

    @Test
    void defaults_are_correct() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();

        assertThat(config.getEventStartPattern(), nullValue());
        assertThat(config.getEventEndPattern(), nullValue());
        assertThat(config.getContinuationLineStartPattern(), nullValue());
        assertThat(config.getContinuationLineEndPattern(), nullValue());
        assertThat(config.getOmitMatchedSection(), equalTo(false));
        assertThat(config.getMaxLines(), equalTo(MultilineInputCodecConfig.DEFAULT_MAX_LINES));
        assertThat(config.getMaxLength(), equalTo(MultilineInputCodecConfig.DEFAULT_MAX_LENGTH));
        assertThat(config.getLineSeparator(), equalTo(MultilineInputCodecConfig.DEFAULT_LINE_SEPARATOR));
        assertThat(config.getConfiguredPatternString(), nullValue());
    }

    @Test
    void isExactlyOnePatternSpecified_returns_true_for_event_start_pattern() {
        final MultilineInputCodecConfig config = MultilineInputCodecConfig.builder()
                .withEventStartPattern("^\\d{4}")
                .build();
        assertThat(config.isExactlyOnePatternSpecified(), equalTo(true));
    }

    @Test
    void isExactlyOnePatternSpecified_returns_true_for_event_end_pattern() {
        final MultilineInputCodecConfig config = MultilineInputCodecConfig.builder()
                .withEventEndPattern("^---$")
                .build();
        assertThat(config.isExactlyOnePatternSpecified(), equalTo(true));
    }

    @Test
    void isExactlyOnePatternSpecified_returns_true_for_continuation_line_start_pattern() {
        final MultilineInputCodecConfig config = MultilineInputCodecConfig.builder()
                .withContinuationLineStartPattern("^\\s")
                .build();
        assertThat(config.isExactlyOnePatternSpecified(), equalTo(true));
    }

    @Test
    void isExactlyOnePatternSpecified_returns_true_for_continuation_line_end_pattern() {
        final MultilineInputCodecConfig config = MultilineInputCodecConfig.builder()
                .withContinuationLineEndPattern("^\\s")
                .build();
        assertThat(config.isExactlyOnePatternSpecified(), equalTo(true));
    }

    @Test
    void isExactlyOnePatternSpecified_returns_false_when_none_specified() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        assertThat(config.isExactlyOnePatternSpecified(), equalTo(false));
    }

    @Test
    void isExactlyOnePatternSpecified_returns_false_when_two_specified() {
        final MultilineInputCodecConfig config = MultilineInputCodecConfig.builder()
                .withEventStartPattern("^\\d{4}")
                .withEventEndPattern("^---$")
                .build();
        assertThat(config.isExactlyOnePatternSpecified(), equalTo(false));
    }

    @Test
    void isValidPattern_returns_true_for_valid_regex() {
        final MultilineInputCodecConfig config = MultilineInputCodecConfig.builder()
                .withEventStartPattern("^\\d{4}-\\d{2}-\\d{2}")
                .build();
        assertThat(config.isValidPattern(), equalTo(true));
    }

    @Test
    void isValidPattern_returns_false_for_invalid_regex() {
        final MultilineInputCodecConfig config = MultilineInputCodecConfig.builder()
                .withEventStartPattern("[invalid(")
                .build();
        assertThat(config.isValidPattern(), equalTo(false));
    }

    @Test
    void isValidPattern_returns_false_when_no_pattern_configured() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        assertThat(config.isValidPattern(), equalTo(false));
    }

    @Test
    void getConfiguredPatternString_returns_null_when_none_specified() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        assertThat(config.getConfiguredPatternString(), nullValue());
    }

    @Test
    void isValidEncoding_returns_true_for_default_utf8() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        assertThat(config.isValidEncoding(), equalTo(true));
    }

    @Test
    void isValidEncoding_returns_true_for_valid_charset() {
        final MultilineInputCodecConfig config = new MultilineInputCodecConfig();
        assertThat(config.isValidEncoding(), equalTo(true));
        assertThat(config.getEncoding().name(), equalTo("UTF-8"));
    }
}
