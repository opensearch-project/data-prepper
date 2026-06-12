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
import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * An implementation of {@link InputCodec} which groups multiple lines from an input stream
 * into single events based on a configurable regex pattern.
 *
 * <p>This is useful for ingesting logs where a single logical event spans multiple lines,
 * such as Java stack traces, Python tracebacks, or any log format where entries begin with
 * a recognizable pattern (e.g., a timestamp).</p>
 *
 * <p>The codec supports two grouping modes via the {@code what} configuration:</p>
 * <ul>
 *   <li>{@code previous}: Continuation lines are appended to the preceding event.</li>
 *   <li>{@code next}: Continuation lines are prepended to the following event.</li>
 * </ul>
 *
 * <p>The {@code negate} option controls which lines are considered continuation lines:</p>
 * <ul>
 *   <li>{@code negate=false}: Lines matching the pattern are continuation lines.</li>
 *   <li>{@code negate=true}: Lines NOT matching the pattern are continuation lines.</li>
 * </ul>
 */
@DataPrepperPlugin(name = "multiline", pluginType = InputCodec.class, pluginConfigurationType = MultilineInputCodecConfig.class)
public class MultilineInputCodec implements InputCodec {

    private static final Logger LOG = LoggerFactory.getLogger(MultilineInputCodec.class);
    static final String MESSAGE_FIELD_NAME = "message";

    private final Pattern pattern;
    private final boolean negate;
    private final MultilineWhat what;
    private final int maxLines;
    private final int maxLength;
    private final String lineSeparator;
    private final EventFactory eventFactory;

    @DataPrepperPluginConstructor
    public MultilineInputCodec(final MultilineInputCodecConfig config, final EventFactory eventFactory) {
        Objects.requireNonNull(config, "config must not be null");
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory must not be null");
        try {
            this.pattern = Pattern.compile(config.getMatch());
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid regex pattern for 'match': " + config.getMatch(), e);
        }
        this.negate = config.getNegate();
        this.what = config.getWhat();
        this.maxLines = config.getMaxLines();
        this.maxLength = config.getMaxLength();
        this.lineSeparator = config.getLineSeparator();
    }

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream, "inputStream must not be null");
        Objects.requireNonNull(eventConsumer, "eventConsumer must not be null");

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            if (what == MultilineWhat.PREVIOUS) {
                parsePreviousMode(reader, eventConsumer);
            } else {
                parseNextMode(reader, eventConsumer);
            }
        }
    }

    /**
     * In PREVIOUS mode, continuation lines are appended to the preceding event.
     * A new event boundary is detected when a line is NOT a continuation line
     * (i.e., it's a "start" line).
     */
    private void parsePreviousMode(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final StringBuilder buffer = new StringBuilder();
        int lineCount = 0;
        String line;

        while ((line = reader.readLine()) != null) {
            final boolean isContinuation = isContinuationLine(line);

            if (!isContinuation && buffer.length() > 0) {
                emitEvent(buffer.toString(), eventConsumer);
                buffer.setLength(0);
                lineCount = 0;
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
            buffer.append(line);
            lineCount++;
        }

        if (buffer.length() > 0) {
            emitEvent(buffer.toString(), eventConsumer);
        }
    }

    /**
     * In NEXT mode, continuation lines are prepended to the following event.
     * A new event boundary is detected when a line is NOT a continuation line,
     * and the buffer (containing prior continuation lines) is combined with this line.
     */
    private void parseNextMode(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final StringBuilder buffer = new StringBuilder();
        int lineCount = 0;
        boolean bufferHasNonContinuation = false;
        String line;

        while ((line = reader.readLine()) != null) {
            final boolean isContinuation = isContinuationLine(line);

            if (!isContinuation) {
                if (bufferHasNonContinuation) {
                    // The buffer already has a complete event (non-continuation at end).
                    // Emit it and start fresh.
                    emitEvent(buffer.toString(), eventConsumer);
                    buffer.setLength(0);
                    lineCount = 0;
                    bufferHasNonContinuation = false;
                }
                // Append this non-continuation line to the buffer (with any preceding continuations).
                if (buffer.length() > 0) {
                    buffer.append(lineSeparator);
                }
                buffer.append(line);
                lineCount++;
                bufferHasNonContinuation = true;
                continue;
            }

            // This is a continuation line.
            if (bufferHasNonContinuation) {
                // Buffer has a complete event ending with non-continuation.
                // Emit it, then start collecting continuations for the next event.
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
            buffer.append(line);
            lineCount++;
        }

        if (buffer.length() > 0) {
            emitEvent(buffer.toString(), eventConsumer);
        }
    }

    /**
     * Determines if a line is a continuation line based on the pattern and negate settings.
     *
     * <p>When {@code negate=false}: a line matching the pattern IS a continuation line.</p>
     * <p>When {@code negate=true}: a line NOT matching the pattern IS a continuation line.</p>
     */
    boolean isContinuationLine(final String line) {
        final boolean matches = pattern.matcher(line).find();
        return negate != matches;
    }

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
