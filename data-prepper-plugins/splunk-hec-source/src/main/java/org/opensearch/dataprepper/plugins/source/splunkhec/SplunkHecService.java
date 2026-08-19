/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.http.BaseHttpService;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecAckResponse;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecMetadataKeyAttributes;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecResponse;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecResponseCode;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import com.linecorp.armeria.common.annotation.Nullable;

@Blocking
public class SplunkHecService implements BaseHttpService {

    static final String REQUESTS_RECEIVED_TOTAL = "requestsReceivedTotal";
    static final String REQUESTS_SUCCESS_TOTAL = "requestsSuccessTotal";
    static final String REQUESTS_FAILED_TOTAL = "requestsFailedTotal";
    static final String REQUESTS_AUTH_FAILED_TOTAL = "requestsAuthFailedTotal";
    static final String EVENTS_RECEIVED_TOTAL = "eventsReceivedTotal";
    static final String EVENTS_WRITTEN_TOTAL = "eventsWrittenTotal";
    static final String REQUEST_SIZE_BYTES = "requestSizeBytes";
    static final String EVENTS_PER_REQUEST = "eventsPerRequest";
    static final String REQUEST_LATENCY = "requestLatency";
    static final String BUFFER_FULL_TOTAL = "bufferFullTotal";
    static final String PARSE_ERRORS_TOTAL = "parseErrorsTotal";

    private static final Logger LOG = LoggerFactory.getLogger(SplunkHecService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String EVENT_FIELD = "event";
    private static final String TIME_FIELD = "time";
    private static final String HOST_FIELD = "host";
    private static final String SOURCE_FIELD = "source";
    private static final String SOURCETYPE_FIELD = "sourcetype";
    private static final String INDEX_FIELD = "index";
    private static final String FIELDS_FIELD = "fields";
    private static final String MESSAGE_FIELD = "message";
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String CHANNEL_HEADER = "X-Splunk-Request-Channel";
    private static final Duration ONE_HOUR = Duration.ofHours(1);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() { };

    private final Buffer<Record<Event>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final HecTokenValidator tokenValidator;
    private final HecEventParser eventParser;
    private final HecAckManager ackManager;
    private final boolean acknowledgements;
    private final boolean flattenEvent;
    private final String rawLineBreaker;
    private final Pattern rawLineBreakerPattern;
    private final String defaultSourcetype;
    private final boolean warnFutureTimestamps;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Duration ackExpiry;

    private final Counter requestsReceivedCounter;
    private final Counter requestsSuccessCounter;
    private final Counter requestsFailedCounter;
    private final Counter requestsAuthFailedCounter;
    private final Counter eventsReceivedCounter;
    private final Counter eventsWrittenCounter;
    private final DistributionSummary requestSizeSummary;
    private final DistributionSummary eventsPerRequestSummary;
    private final Timer requestLatencyTimer;
    private final Counter bufferFullCounter;
    private final Counter parseErrorsCounter;

    public SplunkHecService(final int bufferWriteTimeoutInMillis,
                            final Buffer<Record<Event>> buffer,
                            final PluginMetrics pluginMetrics,
                            final SplunkHecSourceConfig config,
                            final AcknowledgementSetManager acknowledgementSetManager) {
        Objects.requireNonNull(buffer, "buffer must not be null");
        Objects.requireNonNull(pluginMetrics, "pluginMetrics must not be null");
        Objects.requireNonNull(config, "config must not be null");
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.tokenValidator = new HecTokenValidator(config.getTokens());
        this.eventParser = new HecEventParser();
        this.acknowledgements = config.isAcknowledgements();
        this.flattenEvent = config.isFlattenEvent();
        this.rawLineBreaker = config.getRawLineBreaker();
        this.rawLineBreakerPattern = Pattern.compile(Pattern.quote(rawLineBreaker));
        this.defaultSourcetype = config.getDefaultSourcetype();
        this.warnFutureTimestamps = config.isWarnFutureTimestamps();
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.ackExpiry = config.getAckExpiry();

        if (acknowledgements) {
            this.ackManager = new HecAckManager(config.getAckExpiry(), pluginMetrics);
        } else {
            this.ackManager = null;
        }

        this.requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED_TOTAL);
        this.requestsSuccessCounter = pluginMetrics.counter(REQUESTS_SUCCESS_TOTAL);
        this.requestsFailedCounter = pluginMetrics.counter(REQUESTS_FAILED_TOTAL);
        this.requestsAuthFailedCounter = pluginMetrics.counter(REQUESTS_AUTH_FAILED_TOTAL);
        this.eventsReceivedCounter = pluginMetrics.counter(EVENTS_RECEIVED_TOTAL);
        this.eventsWrittenCounter = pluginMetrics.counter(EVENTS_WRITTEN_TOTAL);
        this.requestSizeSummary = pluginMetrics.summary(REQUEST_SIZE_BYTES);
        this.eventsPerRequestSummary = pluginMetrics.summary(EVENTS_PER_REQUEST);
        this.requestLatencyTimer = pluginMetrics.timer(REQUEST_LATENCY);
        this.bufferFullCounter = pluginMetrics.counter(BUFFER_FULL_TOTAL);
        this.parseErrorsCounter = pluginMetrics.counter(PARSE_ERRORS_TOTAL);
    }

    @Post("/event")
    public HttpResponse handleEvent(final ServiceRequestContext serviceRequestContext,
                                    final AggregatedHttpRequest request) {
        return requestLatencyTimer.record(() -> processEventRequest(serviceRequestContext, request));
    }

    @Post("/raw")
    public HttpResponse handleRaw(final ServiceRequestContext serviceRequestContext,
                                  final AggregatedHttpRequest request,
                                  @Param("index") @Nullable String index,
                                  @Param("sourcetype") @Nullable String sourcetype,
                                  @Param("source") @Nullable String source,
                                  @Param("host") @Nullable String host) {
        return requestLatencyTimer.record(() ->
                processRawRequest(serviceRequestContext, request, index, sourcetype, source, host));
    }

    @Post("/ack")
    public HttpResponse handleAck(final AggregatedHttpRequest request) {
        requestsReceivedCounter.increment();
        if (!acknowledgements) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.ACK_DISABLED));
        }

        final AuthResult auth = authenticate(request);
        if (!auth.isAuthenticated()) {
            return buildJsonResponse(auth.status, HecResponse.error(auth.code));
        }

        final String channel = request.headers().get(CHANNEL_HEADER);
        if (channel == null || channel.isBlank()) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.DATA_CHANNEL_MISSING));
        }

        try {
            final Map<String, Object> body = OBJECT_MAPPER.readValue(request.content().toStringUtf8(), MAP_TYPE_REF);
            if (body == null) {
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.INVALID_DATA_FORMAT));
            }
            final Object acksValue = body.get("acks");
            if (!(acksValue instanceof List)) {
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.INVALID_DATA_FORMAT));
            }
            final List<Long> ids = new ArrayList<>();
            for (final Object id : (List<?>) acksValue) {
                if (!(id instanceof Number)) {
                    requestsFailedCounter.increment();
                    return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.INVALID_DATA_FORMAT));
                }
                ids.add(((Number) id).longValue());
            }
            final Map<String, Boolean> results = ackManager.queryAcks(channel, ids);
            requestsSuccessCounter.increment();
            return buildJsonResponse(HttpStatus.OK, new HecAckResponse(results));
        } catch (IOException e) {
            parseErrorsCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.INVALID_DATA_FORMAT));
        }
    }

    @Get("/health")
    public HttpResponse handleHealth() {
        return buildJsonResponse(HttpStatus.OK, HecResponse.error(HecResponseCode.HEC_HEALTHY));
    }

    public void shutdown() {
        if (ackManager != null) {
            ackManager.shutdown();
        }
    }

    private HttpResponse processEventRequest(final ServiceRequestContext serviceRequestContext,
                                             final AggregatedHttpRequest request) {
        requestsReceivedCounter.increment();
        requestSizeSummary.record(request.content().length());

        final AuthResult auth = authenticate(request);
        if (!auth.isAuthenticated()) {
            return buildJsonResponse(auth.status, HecResponse.error(auth.code));
        }
        final String token = auth.token;

        if (acknowledgements) {
            final String channel = request.headers().get(CHANNEL_HEADER);
            if (channel == null || channel.isBlank()) {
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST,
                        HecResponse.error(HecResponseCode.DATA_CHANNEL_MISSING));
            }
            return processEventWithAck(serviceRequestContext, request, token, channel);
        }

        return processEventWithoutAck(serviceRequestContext, request, token);
    }

    private HttpResponse processEventWithAck(final ServiceRequestContext serviceRequestContext,
                                             final AggregatedHttpRequest request,
                                             final String token,
                                             final String channel) {
        final List<Map<String, Object>> parsedEvents;
        try {
            parsedEvents = eventParser.parse(request.content().toStringUtf8());
        } catch (HecParseException e) {
            parseErrorsCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.errorWithInvalidEventNumber(HecResponseCode.INVALID_DATA_FORMAT, e.getEventNumber()));
        }

        if (parsedEvents.isEmpty()) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.error(HecResponseCode.NO_DATA));
        }

        final List<Record<Event>> records;
        try {
            records = mapEventsToRecords(parsedEvents, token, channel);
        } catch (HecEventValidationException e) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.errorWithInvalidEventNumber(HecResponseCode.EVENT_FIELD_REQUIRED, e.getEventNumber()));
        }

        eventsReceivedCounter.increment(records.size());
        eventsPerRequestSummary.record(records.size());

        final long ackId = ackManager.createAck(channel);
        final AcknowledgementSet acknowledgementSet = acknowledgementSetManager.create(
                result -> {
                    if (Boolean.TRUE.equals(result)) {
                        ackManager.confirmAck(channel, ackId);
                    }
                }, ackExpiry);

        for (final Record<Event> record : records) {
            acknowledgementSet.add(record.getData());
        }

        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
        } catch (TimeoutException e) {
            acknowledgementSet.cancel();
            ackManager.removeAck(channel, ackId);
            bufferFullCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.SERVICE_UNAVAILABLE,
                    HecResponse.error(HecResponseCode.SERVER_BUSY));
        } catch (Exception e) {
            acknowledgementSet.cancel();
            ackManager.removeAck(channel, ackId);
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.INTERNAL_SERVER_ERROR,
                    HecResponse.error(HecResponseCode.INTERNAL_SERVER_ERROR));
        }

        acknowledgementSet.complete();
        eventsWrittenCounter.increment(records.size());
        requestsSuccessCounter.increment();
        return buildJsonResponse(HttpStatus.OK, HecResponse.successWithAckId(ackId));
    }

    private HttpResponse processEventWithoutAck(final ServiceRequestContext serviceRequestContext,
                                                final AggregatedHttpRequest request,
                                                final String token) {
        final List<Map<String, Object>> parsedEvents;
        try {
            parsedEvents = eventParser.parse(request.content().toStringUtf8());
        } catch (HecParseException e) {
            parseErrorsCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.errorWithInvalidEventNumber(HecResponseCode.INVALID_DATA_FORMAT, e.getEventNumber()));
        }

        if (parsedEvents.isEmpty()) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.error(HecResponseCode.NO_DATA));
        }

        final String channel = request.headers().get(CHANNEL_HEADER);
        final List<Record<Event>> records;
        try {
            records = mapEventsToRecords(parsedEvents, token, channel);
        } catch (HecEventValidationException e) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.errorWithInvalidEventNumber(HecResponseCode.EVENT_FIELD_REQUIRED, e.getEventNumber()));
        }

        eventsReceivedCounter.increment(records.size());
        eventsPerRequestSummary.record(records.size());

        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
        } catch (TimeoutException e) {
            bufferFullCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.SERVICE_UNAVAILABLE,
                    HecResponse.error(HecResponseCode.SERVER_BUSY));
        } catch (Exception e) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.INTERNAL_SERVER_ERROR,
                    HecResponse.error(HecResponseCode.INTERNAL_SERVER_ERROR));
        }

        eventsWrittenCounter.increment(records.size());
        requestsSuccessCounter.increment();
        return buildJsonResponse(HttpStatus.OK, HecResponse.success());
    }

    private HttpResponse processRawRequest(final ServiceRequestContext serviceRequestContext,
                                           final AggregatedHttpRequest request,
                                           final String index,
                                           final String sourcetype,
                                           final String source,
                                           final String host) {
        requestsReceivedCounter.increment();
        requestSizeSummary.record(request.content().length());

        final AuthResult auth = authenticate(request);
        if (!auth.isAuthenticated()) {
            return buildJsonResponse(auth.status, HecResponse.error(auth.code));
        }
        final String token = auth.token;

        final String channel = request.headers().get(CHANNEL_HEADER);
        if (acknowledgements) {
            if (channel == null || channel.isBlank()) {
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST,
                        HecResponse.error(HecResponseCode.DATA_CHANNEL_MISSING));
            }
        }

        final String body = request.content().toStringUtf8();
        if (body.isEmpty()) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.error(HecResponseCode.NO_DATA));
        }

        final String[] lines = rawLineBreakerPattern.split(body);
        final Optional<HecTokenConfig.HecTokenDefaults> defaults = tokenValidator.getDefaults(token);

        final String effectiveIndex = resolveField(index, defaults.map(HecTokenConfig.HecTokenDefaults::getIndex).orElse(null));
        final String effectiveSourcetype = resolveField(sourcetype,
                defaults.map(HecTokenConfig.HecTokenDefaults::getSourcetype).orElse(defaultSourcetype));
        final String effectiveSource = resolveField(source, defaults.map(HecTokenConfig.HecTokenDefaults::getSource).orElse(null));
        final String effectiveHost = resolveField(host, defaults.map(HecTokenConfig.HecTokenDefaults::getHost).orElse(null));

        final List<Record<Event>> records = new ArrayList<>();
        for (final String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            final Map<String, Object> data = new HashMap<>();
            data.put(MESSAGE_FIELD, line);
            if (effectiveHost != null) {
                data.put(HOST_FIELD, effectiveHost);
            }
            if (effectiveSource != null) {
                data.put(SOURCE_FIELD, effectiveSource);
            }
            if (effectiveSourcetype != null) {
                data.put(SOURCETYPE_FIELD, effectiveSourcetype);
            }
            data.put(TIMESTAMP_FIELD, Instant.now().toString());

            final JacksonEvent event = JacksonEvent.builder()
                    .withEventType(EventType.LOG.toString())
                    .withData(data)
                    .build();

            if (effectiveIndex != null) {
                event.getMetadata().setAttribute(HecMetadataKeyAttributes.INDEX, effectiveIndex);
            }
            if (channel != null) {
                event.getMetadata().setAttribute(HecMetadataKeyAttributes.CHANNEL, channel);
            }
            records.add(new Record<>(event));
        }

        if (records.isEmpty()) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST,
                    HecResponse.error(HecResponseCode.NO_DATA));
        }

        eventsReceivedCounter.increment(records.size());
        eventsPerRequestSummary.record(records.size());

        if (acknowledgements) {
            final long ackId = ackManager.createAck(channel);
            final AcknowledgementSet acknowledgementSet = acknowledgementSetManager.create(
                    result -> {
                        if (Boolean.TRUE.equals(result)) {
                            ackManager.confirmAck(channel, ackId);
                        }
                    }, ackExpiry);

            for (final Record<Event> record : records) {
                acknowledgementSet.add(record.getData());
            }

            try {
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            } catch (TimeoutException e) {
                acknowledgementSet.cancel();
                ackManager.removeAck(channel, ackId);
                bufferFullCounter.increment();
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.SERVICE_UNAVAILABLE,
                        HecResponse.error(HecResponseCode.SERVER_BUSY));
            } catch (Exception e) {
                acknowledgementSet.cancel();
                ackManager.removeAck(channel, ackId);
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.INTERNAL_SERVER_ERROR,
                        HecResponse.error(HecResponseCode.INTERNAL_SERVER_ERROR));
            }

            acknowledgementSet.complete();
            eventsWrittenCounter.increment(records.size());
            requestsSuccessCounter.increment();
            return buildJsonResponse(HttpStatus.OK, HecResponse.successWithAckId(ackId));
        }

        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
        } catch (TimeoutException e) {
            bufferFullCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.SERVICE_UNAVAILABLE,
                    HecResponse.error(HecResponseCode.SERVER_BUSY));
        } catch (Exception e) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.INTERNAL_SERVER_ERROR,
                    HecResponse.error(HecResponseCode.INTERNAL_SERVER_ERROR));
        }

        eventsWrittenCounter.increment(records.size());
        requestsSuccessCounter.increment();
        return buildJsonResponse(HttpStatus.OK, HecResponse.success());
    }

    private List<Record<Event>> mapEventsToRecords(final List<Map<String, Object>> parsedEvents,
                                                   final String token,
                                                   final String channel) {
        final Optional<HecTokenConfig.HecTokenDefaults> defaults = tokenValidator.getDefaults(token);
        final List<Record<Event>> records = new ArrayList<>();

        for (int i = 0; i < parsedEvents.size(); i++) {
            final Map<String, Object> hecEvent = parsedEvents.get(i);
            if (!hecEvent.containsKey(EVENT_FIELD) || hecEvent.get(EVENT_FIELD) == null) {
                throw new HecEventValidationException(i);
            }

            final Object eventValue = hecEvent.get(EVENT_FIELD);
            final Map<String, Object> eventData = buildEventData(hecEvent, eventValue, defaults.orElse(null));
            handleTimestamp(hecEvent, eventData);

            final JacksonEvent event = JacksonEvent.builder()
                    .withEventType(EventType.LOG.toString())
                    .withData(eventData)
                    .build();

            setEventMetadata(event, hecEvent, defaults.orElse(null), channel);
            records.add(new Record<>(event));
        }

        return records;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> buildEventData(final Map<String, Object> hecEvent,
                                               final Object eventValue,
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

        final String host = asString(hecEvent.get(HOST_FIELD));
        final String source = asString(hecEvent.get(SOURCE_FIELD));
        final String sourcetype = asString(hecEvent.get(SOURCETYPE_FIELD));

        final String effectiveHost = resolveField(host, defaults != null ? defaults.getHost() : null);
        final String effectiveSource = resolveField(source, defaults != null ? defaults.getSource() : null);
        final String effectiveSourcetype = resolveField(sourcetype,
                defaults != null && defaults.getSourcetype() != null ? defaults.getSourcetype() : defaultSourcetype);

        if (effectiveHost != null) {
            eventData.put(HOST_FIELD, effectiveHost);
        }
        if (effectiveSource != null) {
            eventData.put(SOURCE_FIELD, effectiveSource);
        }
        if (effectiveSourcetype != null) {
            eventData.put(SOURCETYPE_FIELD, effectiveSourcetype);
        }

        final Object fields = hecEvent.get(FIELDS_FIELD);
        if (fields instanceof Map) {
            eventData.putAll((Map<String, Object>) fields);
        }

        if (defaults != null && defaults.getFields() != null) {
            for (final Map.Entry<String, String> entry : defaults.getFields().entrySet()) {
                eventData.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        return eventData;
    }

    private void handleTimestamp(final Map<String, Object> hecEvent, final Map<String, Object> eventData) {
        final Object timeValue = hecEvent.get(TIME_FIELD);
        if (timeValue != null) {
            final double epochSeconds;
            if (timeValue instanceof Number) {
                epochSeconds = ((Number) timeValue).doubleValue();
            } else {
                try {
                    epochSeconds = Double.parseDouble(timeValue.toString());
                } catch (final NumberFormatException e) {
                    eventData.put(TIMESTAMP_FIELD, Instant.now().toString());
                    return;
                }
            }
            if (!Double.isFinite(epochSeconds)) {
                eventData.put(TIMESTAMP_FIELD, Instant.now().toString());
                return;
            }
            final Instant timestamp;
            try {
                final long seconds = (long) epochSeconds;
                final long nanos = (long) ((epochSeconds - seconds) * 1_000_000_000);
                timestamp = Instant.ofEpochSecond(seconds, nanos);
            } catch (final ArithmeticException | DateTimeException e) {
                eventData.put(TIMESTAMP_FIELD, Instant.now().toString());
                return;
            }

            if (warnFutureTimestamps && timestamp.isAfter(Instant.now().plus(ONE_HOUR))) {
                LOG.warn("Event has timestamp more than 1 hour in the future: {}", timestamp);
            }

            eventData.put(TIMESTAMP_FIELD, timestamp.toString());
        } else {
            eventData.put(TIMESTAMP_FIELD, Instant.now().toString());
        }
    }

    private void setEventMetadata(final JacksonEvent event,
                                  final Map<String, Object> hecEvent,
                                  final HecTokenConfig.HecTokenDefaults defaults,
                                  final String channel) {
        final String index = asString(hecEvent.get(INDEX_FIELD));
        final String effectiveIndex = resolveField(index, defaults != null ? defaults.getIndex() : null);

        if (effectiveIndex != null) {
            event.getMetadata().setAttribute(HecMetadataKeyAttributes.INDEX, effectiveIndex);
        }
        if (channel != null) {
            event.getMetadata().setAttribute(HecMetadataKeyAttributes.CHANNEL, channel);
        }

        final String sourcetype = asString(hecEvent.get(SOURCETYPE_FIELD));
        final String effectiveSourcetype = resolveField(sourcetype,
                defaults != null && defaults.getSourcetype() != null ? defaults.getSourcetype() : defaultSourcetype);
        if (effectiveSourcetype != null) {
            event.getMetadata().setAttribute(HecMetadataKeyAttributes.SOURCETYPE, effectiveSourcetype);
        }

        final String source = asString(hecEvent.get(SOURCE_FIELD));
        final String effectiveSource = resolveField(source, defaults != null ? defaults.getSource() : null);
        if (effectiveSource != null) {
            event.getMetadata().setAttribute(HecMetadataKeyAttributes.SOURCE, effectiveSource);
        }

        final String host = asString(hecEvent.get(HOST_FIELD));
        final String effectiveHost = resolveField(host, defaults != null ? defaults.getHost() : null);
        if (effectiveHost != null) {
            event.getMetadata().setAttribute(HecMetadataKeyAttributes.HOST, effectiveHost);
        }
    }

    private AuthResult authenticate(final AggregatedHttpRequest request) {
        final String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);
        if (authHeader == null || authHeader.isBlank()) {
            requestsAuthFailedCounter.increment();
            return AuthResult.tokenRequired();
        }
        final Optional<String> tokenOpt = tokenValidator.extractToken(authHeader);
        if (tokenOpt.isEmpty()) {
            requestsAuthFailedCounter.increment();
            return AuthResult.invalid();
        }
        final String token = tokenOpt.get();
        if (tokenValidator.isDisabled(token)) {
            requestsAuthFailedCounter.increment();
            return AuthResult.disabled();
        }
        if (!tokenValidator.isValid(token)) {
            requestsAuthFailedCounter.increment();
            return AuthResult.invalidToken();
        }
        return AuthResult.valid(token);
    }

    private String resolveField(final String explicit, final String defaultValue) {
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return defaultValue;
    }

    private static String asString(final Object value) {
        return value == null ? null : value.toString();
    }

    private HttpResponse buildJsonResponse(final HttpStatus status, final Object body) {
        final String json = OBJECT_MAPPER.valueToTree(body).toString();
        return HttpResponse.of(status, MediaType.JSON, json);
    }

    private static final class AuthResult {
        private final String token;
        private final HttpStatus status;
        private final HecResponseCode code;

        private AuthResult(final String token, final HttpStatus status, final HecResponseCode code) {
            this.token = token;
            this.status = status;
            this.code = code;
        }

        static AuthResult valid(final String token) {
            return new AuthResult(token, null, null);
        }

        static AuthResult invalid() {
            return new AuthResult(null, HttpStatus.UNAUTHORIZED, HecResponseCode.TOKEN_INVALID);
        }

        static AuthResult invalidToken() {
            return new AuthResult(null, HttpStatus.FORBIDDEN, HecResponseCode.INVALID_TOKEN);
        }

        static AuthResult tokenRequired() {
            return new AuthResult(null, HttpStatus.UNAUTHORIZED, HecResponseCode.TOKEN_REQUIRED);
        }

        static AuthResult disabled() {
            return new AuthResult(null, HttpStatus.FORBIDDEN, HecResponseCode.TOKEN_DISABLED);
        }

        boolean isAuthenticated() {
            return token != null;
        }
    }

    private static class HecEventValidationException extends RuntimeException {
        private final int eventNumber;

        HecEventValidationException(final int eventNumber) {
            super("Event field is required at event number " + eventNumber);
            this.eventNumber = eventNumber;
        }

        int getEventNumber() {
            return eventNumber;
        }
    }
}
