/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An implementation of {@link InputCodec} which groups multiple lines from an input stream
 * into single events based on a configurable regex pattern.
 *
 * <p>This is useful for ingesting logs where a single logical event spans multiple lines,
 * such as Java stack traces, Python tracebacks, or any log format where entries begin with
 * a recognizable pattern (e.g., a timestamp).</p>
 *
 * <p>The codec supports four mutually exclusive pattern modes:</p>
 * <ul>
 *   <li>{@code event_start_pattern}: A new event begins at each matching line.</li>
 *   <li>{@code event_end_pattern}: An event ends at each matching line (inclusive).</li>
 *   <li>{@code continuation_line_start_pattern}: Matching lines are continuations of the previous event.</li>
 *   <li>{@code continuation_line_end_pattern}: Matching lines are prepended to the next event.</li>
 * </ul>
 */
@DataPrepperPlugin(name = "multiline", pluginType = InputCodec.class, pluginConfigurationType = MultilineInputCodecConfig.class)
public class MultilineInputCodec implements InputCodec {

    private static final Logger LOG = LoggerFactory.getLogger(MultilineInputCodec.class);
    private static final String MESSAGE_FIELD_NAME = "message";

    private final Pattern pattern;
    private final MultilineMode mode;
    private final boolean omitMatchedSection;
    private final int maxLines;
    private final int maxLength;
    private final String lineSeparator;
    private final Charset encoding;
    private final EventFactory eventFactory;

    @DataPrepperPluginConstructor
    public MultilineInputCodec(final MultilineInputCodecConfig config, final EventFactory eventFactory) {
        Objects.requireNonNull(config, "config must not be null");
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory must not be null");

        this.pattern = config.getCompiledPattern();
        if (this.pattern == null) {
            throw new IllegalArgumentException("A valid pattern must be configured");
        }

        this.mode = resolveMode(config);
        this.omitMatchedSection = config.getOmitMatchedSection();
        this.maxLines = config.getMaxLines();
        this.maxLength = config.getMaxLength();
        this.lineSeparator = config.getLineSeparator();
        this.encoding = config.getEncoding();
    }

    private static MultilineMode resolveMode(final MultilineInputCodecConfig config) {
        if (config.getEventStartPattern() != null) {
            return MultilineMode.EVENT_START;
        } else if (config.getEventEndPattern() != null) {
            return MultilineMode.EVENT_END;
        } else if (config.getContinuationLineStartPattern() != null) {
            return MultilineMode.CONTINUATION_START;
        } else {
            return MultilineMode.CONTINUATION_END;
        }
    }

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream, "inputStream must not be null");
        Objects.requireNonNull(eventConsumer, "eventConsumer must not be null");

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, encoding))) {
            switch (mode) {
                case EVENT_START:
                    parseEventStartMode(reader, eventConsumer);
                    break;
                case EVENT_END:
                    parseEventEndMode(reader, eventConsumer);
                    break;
                case CONTINUATION_START:
                    parseContinuationStartMode(reader, eventConsumer);
                    break;
                case CONTINUATION_END:
                    parseContinuationEndMode(reader, eventConsumer);
                    break;
                default:
                    throw new IllegalStateException("Unknown multiline mode: " + mode);
            }
        }
    }

    /**
     * EVENT_START mode: A new event begins at each line matching the pattern.
     * Non-matching lines are continuations of the preceding event.
     */
    private void parseEventStartMode(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final StringBuilder buffer = new StringBuilder();
        int lineCount = 0;
        String line;

        while ((line = reader.readLine()) != null) {
            final boolean matches = pattern.matcher(line).find();

            if (matches || shouldFlush(buffer, lineCount, line)) {
                if (buffer.length() > 0) {
                    emitEvent(buffer.toString(), eventConsumer);
                    buffer.setLength(0);
                    lineCount = 0;
                }
            }

            if (buffer.length() > 0) {
                buffer.append(lineSeparator);
            }
            buffer.append(processLine(line, matches));
            lineCount++;
        }

        if (buffer.length() > 0) {
            emitEvent(buffer.toString(), eventConsumer);
        }
    }

    /**
     * EVENT_END mode: An event ends at each line matching the pattern (inclusive).
     * The matching line is included in the current event, then a new event begins.
     */
    private void parseEventEndMode(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final StringBuilder buffer = new StringBuilder();
        int lineCount = 0;
        String line;

        while ((line = reader.readLine()) != null) {
            final boolean matches = pattern.matcher(line).find();

            if (shouldFlush(buffer, lineCount, line)) {
                if (buffer.length() > 0) {
                    emitEvent(buffer.toString(), eventConsumer);
                    buffer.setLength(0);
                    lineCount = 0;
                }
            }

            if (buffer.length() > 0) {
                buffer.append(lineSeparator);
            }
            buffer.append(processLine(line, matches));
            lineCount++;

            if (matches) {
                emitEvent(buffer.toString(), eventConsumer);
                buffer.setLength(0);
                lineCount = 0;
            }
        }

        if (buffer.length() > 0) {
            emitEvent(buffer.toString(), eventConsumer);
        }
    }

    /**
     * CONTINUATION_START mode: Lines matching the pattern are continuations of the previous event.
     * Non-matching lines start new events.
     */
    private void parseContinuationStartMode(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final StringBuilder buffer = new StringBuilder();
        int lineCount = 0;
        String line;

        while ((line = reader.readLine()) != null) {
            final boolean matches = pattern.matcher(line).find();

            if (!matches || shouldFlush(buffer, lineCount, line)) {
                if (buffer.length() > 0) {
                    emitEvent(buffer.toString(), eventConsumer);
                    buffer.setLength(0);
                    lineCount = 0;
                }
            }

            if (buffer.length() > 0) {
                buffer.append(lineSeparator);
            }
            buffer.append(processLine(line, matches));
            lineCount++;
        }

        if (buffer.length() > 0) {
            emitEvent(buffer.toString(), eventConsumer);
        }
    }

    /**
     * CONTINUATION_END mode: Lines matching the pattern are prepended to the next event.
     * Non-matching lines complete the current event.
     */
    private void parseContinuationEndMode(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final StringBuilder buffer = new StringBuilder();
        int lineCount = 0;
        boolean bufferHasNonContinuation = false;
        String line;

        while ((line = reader.readLine()) != null) {
            final boolean matches = pattern.matcher(line).find();

            if (!matches) {
                if (bufferHasNonContinuation) {
                    emitEvent(buffer.toString(), eventConsumer);
                    buffer.setLength(0);
                    lineCount = 0;
                    bufferHasNonContinuation = false;
                }
                if (buffer.length() > 0) {
                    buffer.append(lineSeparator);
                }
                buffer.append(processLine(line, false));
                lineCount++;
                bufferHasNonContinuation = true;
                continue;
            }

            if (bufferHasNonContinuation) {
                emitEvent(buffer.toString(), eventConsumer);
                buffer.setLength(0);
                lineCount = 0;
                bufferHasNonContinuation = false;
            }

            if (shouldFlush(buffer, lineCount, line)) {
                if (buffer.length() > 0) {
                    emitEvent(buffer.toString(), eventConsumer);
                    buffer.setLength(0);
                    lineCount = 0;
                }
            }

            if (buffer.length() > 0) {
                buffer.append(lineSeparator);
            }
            buffer.append(processLine(line, matches));
            lineCount++;
        }

        if (buffer.length() > 0) {
            emitEvent(buffer.toString(), eventConsumer);
        }
    }

    private String processLine(final String line, final boolean matches) {
        if (!omitMatchedSection || !matches) {
            return line;
        }
        final Matcher matcher = pattern.matcher(line);
        return matcher.replaceFirst("");
    }

    /**
     * Determines if the buffer should be flushed before appending the next line.
     * Note: if a single line exceeds max_length on its own, it will still be emitted
     * as a complete event without truncation.
     */
    private boolean shouldFlush(final StringBuilder buffer, final int lineCount, final String nextLine) {
        if (lineCount >= maxLines) {
            LOG.debug("Flushing multiline event due to max_lines limit of {}", maxLines);
            return true;
        }
        if (buffer.length() + lineSeparator.length() + nextLine.length() > maxLength) {
            LOG.debug("Flushing multiline event due to max_length limit of {}", maxLength);
            return true;
        }
        return false;
    }

    private void emitEvent(final String message, final Consumer<Record<Event>> eventConsumer) {
        final Log event = eventFactory.eventBuilder(LogEventBuilder.class)
                .withData(Collections.singletonMap(MESSAGE_FIELD_NAME, message))
                .build();
        eventConsumer.accept(new Record<>(event));
    }
}
