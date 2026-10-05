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
import com.linecorp.armeria.common.annotation.Nullable;
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
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecAckResponse;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecResponse;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecResponseCode;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String ACKS_FIELD = "acks";
    private static final String CHANNEL_HEADER = "X-Splunk-Request-Channel";
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() { };

    private final Buffer<Record<Event>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final HecTokenValidator tokenValidator;
    private final HecEventParser eventParser;
    private final HecEventBuilder eventBuilder;
    private final HecAckManager ackManager;
    private final boolean acknowledgements;
    private final Pattern rawLineBreakerPattern;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Duration acknowledgementExpiry;

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
                            final AcknowledgementSetManager acknowledgementSetManager,
                            final EventFactory eventFactory) {
        Objects.requireNonNull(buffer, "buffer must not be null");
        Objects.requireNonNull(pluginMetrics, "pluginMetrics must not be null");
        Objects.requireNonNull(config, "config must not be null");
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.tokenValidator = new HecTokenValidator(config.getTokens());
        this.eventParser = new HecEventParser();
        this.eventBuilder = new HecEventBuilder(eventFactory, config);
        this.acknowledgements = config.isAcknowledgements();
        this.rawLineBreakerPattern = Pattern.compile(Pattern.quote(config.getRawLineBreaker()));
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.acknowledgementExpiry = config.getAcknowledgementExpiry();

        if (acknowledgements) {
            this.ackManager = new HecAckManager(config.getAcknowledgementExpiry(), pluginMetrics);
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
    public HttpResponse handleEvent(final AggregatedHttpRequest request) {
        return handleIngestRequest(request, (content, defaults, channel) ->
                eventBuilder.buildFromHecEvents(eventParser.parse(content), defaults, channel));
    }

    @Post("/raw")
    public HttpResponse handleRaw(final AggregatedHttpRequest request,
                                  @Param("index") @Nullable final String index,
                                  @Param("sourcetype") @Nullable final String sourcetype,
                                  @Param("source") @Nullable final String source,
                                  @Param("host") @Nullable final String host) {
        return handleIngestRequest(request, (content, defaults, channel) ->
                eventBuilder.buildFromRawLines(rawLineBreakerPattern.split(content),
                        index, sourcetype, source, host, defaults, channel));
    }

    @Post("/ack")
    public HttpResponse handleAck(final AggregatedHttpRequest request) {
        return requestLatencyTimer.record(() -> processAckRequest(request));
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

    private HttpResponse handleIngestRequest(final AggregatedHttpRequest request, final RecordBuilder recordBuilder) {
        return requestLatencyTimer.record(() -> {
            requestsReceivedCounter.increment();
            requestSizeSummary.record(request.content().length());

            final AuthResult auth = authenticate(request);
            if (!auth.isAuthenticated()) {
                return buildJsonResponse(auth.status, HecResponse.error(auth.code));
            }

            final String channel = request.headers().get(CHANNEL_HEADER);
            if (acknowledgements && isBlank(channel)) {
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.DATA_CHANNEL_MISSING));
            }

            final HecTokenConfig.HecTokenDefaults defaults = tokenValidator.getDefaults(auth.token).orElse(null);

            final List<Record<Event>> records;
            try {
                records = recordBuilder.build(request.content().toStringUtf8(), defaults, channel);
            } catch (final HecParseException e) {
                parseErrorsCounter.increment();
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST,
                        HecResponse.errorWithInvalidEventNumber(HecResponseCode.INVALID_DATA_FORMAT, e.getEventNumber()));
            } catch (final HecEventValidationException e) {
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST,
                        HecResponse.errorWithInvalidEventNumber(HecResponseCode.EVENT_FIELD_REQUIRED, e.getEventNumber()));
            }

            if (records.isEmpty()) {
                requestsFailedCounter.increment();
                return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.NO_DATA));
            }

            return writeRecords(records, channel);
        });
    }

    private HttpResponse writeRecords(final List<Record<Event>> records, final String channel) {
        eventsReceivedCounter.increment(records.size());
        eventsPerRequestSummary.record(records.size());

        Long ackId = null;
        AcknowledgementSet acknowledgementSet = null;
        if (acknowledgements) {
            final long createdAckId = ackManager.createAck(channel);
            ackId = createdAckId;
            acknowledgementSet = acknowledgementSetManager.create(result -> {
                if (Boolean.TRUE.equals(result)) {
                    ackManager.confirmAck(channel, createdAckId);
                } else {
                    ackManager.removeAck(channel, createdAckId);
                }
            }, acknowledgementExpiry);

            for (final Record<Event> record : records) {
                acknowledgementSet.add(record.getData());
            }
        }

        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
        } catch (final TimeoutException e) {
            discardAcknowledgement(acknowledgementSet, channel, ackId);
            bufferFullCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.SERVICE_UNAVAILABLE, HecResponse.error(HecResponseCode.SERVER_BUSY));
        } catch (final Exception e) {
            discardAcknowledgement(acknowledgementSet, channel, ackId);
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, HecResponse.error(HecResponseCode.INTERNAL_SERVER_ERROR));
        }

        if (acknowledgementSet != null) {
            acknowledgementSet.complete();
        }
        eventsWrittenCounter.increment(records.size());
        requestsSuccessCounter.increment();

        return ackId == null
                ? buildJsonResponse(HttpStatus.OK, HecResponse.success())
                : buildJsonResponse(HttpStatus.OK, HecResponse.successWithAckId(ackId));
    }

    private void discardAcknowledgement(final AcknowledgementSet acknowledgementSet,
                                        final String channel,
                                        final Long ackId) {
        if (acknowledgementSet == null) {
            return;
        }
        acknowledgementSet.cancel();
        ackManager.removeAck(channel, ackId);
    }

    private HttpResponse processAckRequest(final AggregatedHttpRequest request) {
        requestsReceivedCounter.increment();

        final AuthResult auth = authenticate(request);
        if (!auth.isAuthenticated()) {
            return buildJsonResponse(auth.status, HecResponse.error(auth.code));
        }

        if (!acknowledgements) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.ACK_DISABLED));
        }

        final String channel = request.headers().get(CHANNEL_HEADER);
        if (isBlank(channel)) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.DATA_CHANNEL_MISSING));
        }

        final List<Long> ids;
        try {
            ids = parseAckIds(request.content().toStringUtf8());
        } catch (final IOException e) {
            parseErrorsCounter.increment();
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.INVALID_DATA_FORMAT));
        } catch (final IllegalArgumentException e) {
            requestsFailedCounter.increment();
            return buildJsonResponse(HttpStatus.BAD_REQUEST, HecResponse.error(HecResponseCode.INVALID_DATA_FORMAT));
        }

        final Map<String, Boolean> results = ackManager.queryAcks(channel, ids);
        requestsSuccessCounter.increment();
        return buildJsonResponse(HttpStatus.OK, new HecAckResponse(results));
    }

    private List<Long> parseAckIds(final String content) throws IOException {
        final Map<String, Object> body = OBJECT_MAPPER.readValue(content, MAP_TYPE_REF);
        if (body == null) {
            throw new IllegalArgumentException("The acknowledgement request body must be a JSON object.");
        }
        final Object acksValue = body.get(ACKS_FIELD);
        if (!(acksValue instanceof List)) {
            throw new IllegalArgumentException("The acknowledgement request must provide an acks array.");
        }
        final List<Long> ids = new ArrayList<>();
        for (final Object id : (List<?>) acksValue) {
            if (!(id instanceof Number)) {
                throw new IllegalArgumentException("Every acknowledgement id must be a number.");
            }
            ids.add(((Number) id).longValue());
        }
        return ids;
    }

    private AuthResult authenticate(final AggregatedHttpRequest request) {
        final String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);
        if (isBlank(authHeader)) {
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

    private static boolean isBlank(final String value) {
        return value == null || value.isBlank();
    }

    private HttpResponse buildJsonResponse(final HttpStatus status, final Object body) {
        final String json = OBJECT_MAPPER.valueToTree(body).toString();
        return HttpResponse.of(status, MediaType.JSON, json);
    }

    @FunctionalInterface
    private interface RecordBuilder {
        List<Record<Event>> build(String content, HecTokenConfig.HecTokenDefaults defaults, String channel)
                throws HecParseException;
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
}
