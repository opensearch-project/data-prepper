/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Configuration class for the multiline input codec.
 *
 * <p>The multiline codec groups consecutive lines from an input stream into a single event
 * based on a regex pattern. This is useful for log formats where a single logical event
 * spans multiple lines (e.g., Java stack traces, multi-line application logs).</p>
 *
 * <p>Example configuration for Java stack traces:</p>
 * <pre>
 * codec:
 *   multiline:
 *     match: "^\\s+(at |\\.\\.\\.|Caused by:)"
 *     negate: false
 *     what: previous
 * </pre>
 *
 * <p>Example configuration for timestamp-prefixed logs:</p>
 * <pre>
 * codec:
 *   multiline:
 *     match: "^\\d{4}-\\d{2}-\\d{2}"
 *     negate: true
 *     what: previous
 * </pre>
 */
public class MultilineInputCodecConfig {

    static final int DEFAULT_MAX_LINES = 500;
    static final int DEFAULT_MAX_LENGTH = 10000;
    static final String DEFAULT_LINE_SEPARATOR = "\n";

    @NotEmpty(message = "match must not be empty")
    @JsonProperty("match")
    private String match;

    @NotNull(message = "negate must not be null")
    @JsonProperty("negate")
    private Boolean negate = false;

    @NotNull(message = "what must not be null")
    @JsonProperty("what")
    private MultilineWhat what = MultilineWhat.PREVIOUS;

    @Min(value = 1, message = "max_lines must be at least 1")
    @JsonProperty("max_lines")
    private int maxLines = DEFAULT_MAX_LINES;

    @Min(value = 1, message = "max_length must be at least 1")
    @JsonProperty("max_length")
    private int maxLength = DEFAULT_MAX_LENGTH;

    @NotNull(message = "line_separator must not be null")
    @JsonProperty("line_separator")
    private String lineSeparator = DEFAULT_LINE_SEPARATOR;

    /**
     * The regex pattern used to identify line boundaries.
     *
     * @return The regex pattern string.
     */
    public String getMatch() {
        return match;
    }

    /**
     * Whether to negate the pattern match.
     * <p>When false: lines matching the pattern are considered continuation lines.</p>
     * <p>When true: lines NOT matching the pattern are considered continuation lines.</p>
     *
     * @return true if the pattern should be negated.
     */
    public Boolean getNegate() {
        return negate;
    }

    /**
     * Defines whether unmatched (continuation) lines belong to the previous or next event.
     *
     * @return The multiline grouping direction.
     */
    public MultilineWhat getWhat() {
        return what;
    }

    /**
     * The maximum number of lines that can be combined into a single event.
     * When this limit is reached, the accumulated lines are flushed as an event
     * and a new accumulation begins.
     *
     * @return The maximum number of lines per event.
     */
    public int getMaxLines() {
        return maxLines;
    }

    /**
     * The maximum character length of a combined multiline event.
     * When this limit is reached, the accumulated lines are flushed as an event.
     *
     * @return The maximum character length per event.
     */
    public int getMaxLength() {
        return maxLength;
    }

    /**
     * The separator string to use when joining multiple lines into a single event message.
     *
     * @return The line separator string.
     */
    public String getLineSeparator() {
        return lineSeparator;
    }

    @AssertTrue(message = "match must be a valid regular expression")
    boolean isValidPattern() {
        if (match == null || match.isEmpty()) {
            return false;
        }
        try {
            Pattern.compile(match);
            return true;
        } catch (final PatternSyntaxException e) {
            return false;
        }
    }
}
