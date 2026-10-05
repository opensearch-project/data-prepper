/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecMetadataKeyAttributes;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Creates Data Prepper {@link Event}s from the payloads accepted by the Splunk HEC endpoints.
 * <p>
 * This holds all of the metadata resolution, event flattening, and timestamp handling so that
 * those behaviors can be unit tested directly instead of only through an HTTP port.
 */
public class HecEventBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(HecEventBuilder.class);

    private static final String EVENT_FIELD = "event";
    private static final String TIME_FIELD = "time";
    private static final String HOST_FIELD = "host";
    private static final String SOURCE_FIELD = "source";
    private static final String SOURCETYPE_FIELD = "sourcetype";
    private static final String INDEX_FIELD = "index";
    private static final String FIELDS_FIELD = "fields";
    private static final String MESSAGE_FIELD = "message";
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final Duration ONE_HOUR = Duration.ofHours(1);

    private final EventFactory eventFactory;
    private final boolean flattenEvent;
    private final String defaultSourcetype;
    private final boolean warnFutureTimestamps;

    public HecEventBuilder(final EventFactory eventFactory, final SplunkHecSourceConfig config) {
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        this.flattenEvent = config.isFlattenEvent();
        this.defaultSourcetype = config.getDefaultSourcetype();
        this.warnFutureTimestamps = config.isWarnFutureTimestamps();
    }

    /**
     * Builds records for the <i>/event</i> endpoint from the parsed HEC event envelopes.
     *
     * @param parsedEvents the HEC event envelopes in request order
     * @param defaults     the per-token defaults, or null when the token declares none
     * @param channel      the data channel from the request, or null when absent
     * @return the records to write to the buffer
     * @throws HecEventValidationException when an envelope omits the required event field
     */
    public List<Record<Event>> buildFromHecEvents(final List<Map<String, Object>> parsedEvents,
                                                  final HecTokenConfig.HecTokenDefaults defaults,
                                                  final String channel) {
        final List<Record<Event>> records = new ArrayList<>(parsedEvents.size());

        for (int i = 0; i < parsedEvents.size(); i++) {
            final Map<String, Object> hecEvent = parsedEvents.get(i);
            final Object eventValue = hecEvent.get(EVENT_FIELD);
            if (eventValue == null) {
                throw new HecEventValidationException(i);
            }

            final ResolvedMetadata metadata = resolveMetadata(
                    asString(hecEvent.get(INDEX_FIELD)),
                    asString(hecEvent.get(SOURCETYPE_FIELD)),
                    asString(hecEvent.get(SOURCE_FIELD)),
                    asString(hecEvent.get(HOST_FIELD)),
                    defaults);

            final Map<String, Object> eventData = buildEventData(hecEvent, eventValue, metadata, defaults);
            eventData.put(TIMESTAMP_FIELD, resolveTimestamp(hecEvent.get(TIME_FIELD)).toString());

            records.add(new Record<>(createEvent(eventData, metadata, channel)));
        }

        return records;
    }

    /**
     * Builds records for the <i>/raw</i> endpoint, one record per non-empty line.
     *
     * @param lines      the request body already split on the configured line breaker
     * @param index      the index query parameter, or null when absent
     * @param sourcetype the sourcetype query parameter, or null when absent
     * @param source     the source query parameter, or null when absent
     * @param host       the host query parameter, or null when absent
     * @param defaults   the per-token defaults, or null when the token declares none
     * @param channel    the data channel from the request, or null when absent
     * @return the records to write to the buffer
     */
    public List<Record<Event>> buildFromRawLines(final String[] lines,
                                                 final String index,
                                                 final String sourcetype,
                                                 final String source,
                                                 final String host,
                                                 final HecTokenConfig.HecTokenDefaults defaults,
                                                 final String channel) {
        final ResolvedMetadata metadata = resolveMetadata(index, sourcetype, source, host, defaults);
        final List<Record<Event>> records = new ArrayList<>(lines.length);

        for (final String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            final Map<String, Object> eventData = new HashMap<>();
            eventData.put(MESSAGE_FIELD, line);
            putMetadataFields(eventData, metadata);
            applyDefaultFields(eventData, defaults);
            eventData.put(TIMESTAMP_FIELD, Instant.now().toString());

            records.add(new Record<>(createEvent(eventData, metadata, channel)));
        }

        return records;
    }

    private Event createEvent(final Map<String, Object> eventData,
                              final ResolvedMetadata metadata,
                              final String channel) {
        final Event event = eventFactory.eventBuilder(EventBuilder.class)
                .withEventType(EventType.LOG.toString())
                .withData(eventData)
                .build();

        setAttributeIfPresent(event, HecMetadataKeyAttributes.INDEX, metadata.index);
        setAttributeIfPresent(event, HecMetadataKeyAttributes.SOURCETYPE, metadata.sourcetype);
        setAttributeIfPresent(event, HecMetadataKeyAttributes.SOURCE, metadata.source);
        setAttributeIfPresent(event, HecMetadataKeyAttributes.HOST, metadata.host);
        setAttributeIfPresent(event, HecMetadataKeyAttributes.CHANNEL, channel);

        return event;
    }

    private static void setAttributeIfPresent(final Event event, final String key, final String value) {
        if (value != null) {
            event.getMetadata().setAttribute(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> buildEventData(final Map<String, Object> hecEvent,
                                               final Object eventValue,
                                               final ResolvedMetadata metadata,
                                               final HecTokenConfig.HecTokenDefaults defaults) {
        final Map<String, Object> eventData = new HashMap<>();

        if (eventValue instanceof Map) {
            if (flattenEvent) {
                eventData.putAll((Map<String, Object>) eventValue);
            } else {
                eventData.put(EVENT_FIELD, eventValue);
            }
        } else if (eventValue instanceof String) {
            eventData.put(MESSAGE_FIELD, eventValue);
        } else {
            eventData.put(EVENT_FIELD, eventValue);
        }

        putMetadataFields(eventData, metadata);

        final Object fields = hecEvent.get(FIELDS_FIELD);
        if (fields instanceof Map) {
            eventData.putAll((Map<String, Object>) fields);
        }

        applyDefaultFields(eventData, defaults);

        return eventData;
    }

    private static void putMetadataFields(final Map<String, Object> eventData, final ResolvedMetadata metadata) {
        if (metadata.host != null) {
            eventData.put(HOST_FIELD, metadata.host);
        }
        if (metadata.source != null) {
            eventData.put(SOURCE_FIELD, metadata.source);
        }
        if (metadata.sourcetype != null) {
            eventData.put(SOURCETYPE_FIELD, metadata.sourcetype);
        }
    }

    private static void applyDefaultFields(final Map<String, Object> eventData,
                                           final HecTokenConfig.HecTokenDefaults defaults) {
        if (defaults == null) {
            return;
        }
        for (final Map.Entry<String, String> entry : defaults.getFields().entrySet()) {
            eventData.putIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    private ResolvedMetadata resolveMetadata(final String index,
                                             final String sourcetype,
                                             final String source,
                                             final String host,
                                             final HecTokenConfig.HecTokenDefaults defaults) {
        final String defaultIndex = defaults == null ? null : defaults.getIndex();
        final String defaultSource = defaults == null ? null : defaults.getSource();
        final String defaultHost = defaults == null ? null : defaults.getHost();
        final String tokenSourcetype = defaults == null ? null : defaults.getSourcetype();

        return new ResolvedMetadata(
                resolveField(index, defaultIndex),
                resolveField(sourcetype, tokenSourcetype != null ? tokenSourcetype : defaultSourcetype),
                resolveField(source, defaultSource),
                resolveField(host, defaultHost));
    }

    private Instant resolveTimestamp(final Object timeValue) {
        if (timeValue == null) {
            return Instant.now();
        }

        final double epochSeconds;
        if (timeValue instanceof Number) {
            epochSeconds = ((Number) timeValue).doubleValue();
        } else {
            try {
                epochSeconds = Double.parseDouble(timeValue.toString());
            } catch (final NumberFormatException e) {
                return Instant.now();
            }
        }

        if (!Double.isFinite(epochSeconds)) {
            return Instant.now();
        }

        final Instant timestamp;
        try {
            final long seconds = (long) epochSeconds;
            final long nanos = (long) ((epochSeconds - seconds) * 1_000_000_000);
            timestamp = Instant.ofEpochSecond(seconds, nanos);
        } catch (final ArithmeticException | DateTimeException e) {
            return Instant.now();
        }

        if (warnFutureTimestamps && timestamp.isAfter(Instant.now().plus(ONE_HOUR))) {
            LOG.warn("Event has timestamp more than 1 hour in the future: {}", timestamp);
        }

        return timestamp;
    }

    private static String resolveField(final String explicit, final String defaultValue) {
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return defaultValue;
    }

    private static String asString(final Object value) {
        return value == null ? null : value.toString();
    }

    private static final class ResolvedMetadata {
        private final String index;
        private final String sourcetype;
        private final String source;
        private final String host;

        private ResolvedMetadata(final String index, final String sourcetype, final String source, final String host) {
            this.index = index;
            this.sourcetype = sourcetype;
            this.source = source;
            this.host = host;
        }
    }
}
