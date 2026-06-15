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
import jakarta.validation.constraints.NotNull;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Configuration class for the multiline input codec.
 *
 * <p>The multiline codec groups consecutive lines from an input stream into a single event
 * based on a regex pattern. Exactly one of the four pattern fields must be specified:</p>
 * <ul>
 *   <li>{@code event_start_pattern}: A new event begins at each line matching this pattern.</li>
 *   <li>{@code event_end_pattern}: An event ends at each line matching this pattern (inclusive).</li>
 *   <li>{@code continuation_line_start_pattern}: Lines matching this pattern are continuations of the previous event.</li>
 *   <li>{@code continuation_line_end_pattern}: Lines matching this pattern are prepended to the next event.</li>
 * </ul>
 *
 * <p>Example configuration for Java stack traces:</p>
 * <pre>
 * codec:
 *   multiline:
 *     event_start_pattern: "^\\d{4}-\\d{2}-\\d{2}"
 * </pre>
 */
public class MultilineInputCodecConfig {

    static final int DEFAULT_MAX_LINES = 500;
    static final int DEFAULT_MAX_LENGTH = 10000;
    static final String DEFAULT_LINE_SEPARATOR = "\n";

    @JsonProperty("event_start_pattern")
    private String eventStartPattern;

    @JsonProperty("event_end_pattern")
    private String eventEndPattern;

    @JsonProperty("continuation_line_start_pattern")
    private String continuationLineStartPattern;

    @JsonProperty("continuation_line_end_pattern")
    private String continuationLineEndPattern;

    @JsonProperty("omit_matched_section")
    private boolean omitMatchedSection = false;

    @Min(value = 1, message = "max_lines must be at least 1")
    @JsonProperty("max_lines")
    private int maxLines = DEFAULT_MAX_LINES;

    @Min(value = 1, message = "max_length must be at least 1")
    @JsonProperty("max_length")
    private int maxLength = DEFAULT_MAX_LENGTH;

    @NotNull(message = "line_separator must not be null")
    @JsonProperty("line_separator")
    private String lineSeparator = DEFAULT_LINE_SEPARATOR;

    @JsonProperty("encoding")
    private String encoding = StandardCharsets.UTF_8.name();

    private Pattern compiledPattern;
    private Charset encodingCharset;

    public String getEventStartPattern() {
        return eventStartPattern;
    }

    public String getEventEndPattern() {
        return eventEndPattern;
    }

    public String getContinuationLineStartPattern() {
        return continuationLineStartPattern;
    }

    public String getContinuationLineEndPattern() {
        return continuationLineEndPattern;
    }

    public boolean getOmitMatchedSection() {
        return omitMatchedSection;
    }

    public int getMaxLines() {
        return maxLines;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }

    /**
     * Returns the validated Charset. The encoding is validated once during
     * bean validation and stored to avoid repeated parsing.
     *
     * @return The validated Charset.
     */
    public Charset getEncoding() {
        return encodingCharset;
    }

    /**
     * Returns the compiled regex pattern. The pattern is compiled once during validation
     * and reused to avoid duplicate compilation.
     *
     * @return The compiled Pattern.
     */
    public Pattern getCompiledPattern() {
        return compiledPattern;
    }

    @AssertTrue(message = "Exactly one pattern field must be specified: event_start_pattern, event_end_pattern, " +
            "continuation_line_start_pattern, or continuation_line_end_pattern")
    boolean isExactlyOnePatternSpecified() {
        int count = 0;
        if (eventStartPattern != null) count++;
        if (eventEndPattern != null) count++;
        if (continuationLineStartPattern != null) count++;
        if (continuationLineEndPattern != null) count++;
        return count == 1;
    }

    @AssertTrue(message = "The specified pattern must be a valid regular expression")
    boolean isValidPattern() {
        final String patternString = getConfiguredPatternString();
        if (patternString == null || patternString.isEmpty()) {
            return false;
        }
        try {
            compiledPattern = Pattern.compile(patternString);
            return true;
        } catch (final PatternSyntaxException e) {
            return false;
        }
    }

    @AssertTrue(message = "The specified encoding must be a valid charset")
    boolean isValidEncoding() {
        if (encoding == null || encoding.isEmpty()) {
            return false;
        }
        try {
            encodingCharset = Charset.forName(encoding);
            return true;
        } catch (final IllegalCharsetNameException | UnsupportedCharsetException e) {
            return false;
        }
    }

    String getConfiguredPatternString() {
        if (eventStartPattern != null) return eventStartPattern;
        if (eventEndPattern != null) return eventEndPattern;
        if (continuationLineStartPattern != null) return continuationLineStartPattern;
        if (continuationLineEndPattern != null) return continuationLineEndPattern;
        return null;
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private final MultilineInputCodecConfig config = new MultilineInputCodecConfig();

        Builder withEventStartPattern(final String pattern) {
            config.eventStartPattern = pattern;
            return this;
        }

        Builder withEventEndPattern(final String pattern) {
            config.eventEndPattern = pattern;
            return this;
        }

        Builder withContinuationLineStartPattern(final String pattern) {
            config.continuationLineStartPattern = pattern;
            return this;
        }

        Builder withContinuationLineEndPattern(final String pattern) {
            config.continuationLineEndPattern = pattern;
            return this;
        }

        MultilineInputCodecConfig build() {
            return config;
        }
    }
}
