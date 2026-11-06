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
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecMetadataKeyAttributes;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SplunkHecServiceTest {

    private static final EventFactory TEST_EVENT_FACTORY = TestEventFactory.getTestEventFactory();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String validToken;
    private String authHeader;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SplunkHecSourceConfig config;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private Counter requestsReceivedCounter;
    @Mock
    private Counter requestsSuccessCounter;
    @Mock
    private Counter requestsFailedCounter;
    @Mock
    private Counter requestsAuthFailedCounter;
    @Mock
    private Counter eventsReceivedCounter;
    @Mock
    private Counter eventsWrittenCounter;
    @Mock
    private Counter bufferFullCounter;
    @Mock
    private Counter parseErrorsCounter;
    @Mock
    private DistributionSummary requestSizeSummary;
    @Mock
    private DistributionSummary eventsPerRequestSummary;
    @Mock
    private Timer requestLatencyTimer;

    private SplunkHecService service;

    @BeforeEach
    void setUp() {
        validToken = UUID.randomUUID().toString();
        authHeader = "Splunk " + validToken;

        when(pluginMetrics.counter(SplunkHecService.REQUESTS_RECEIVED_TOTAL)).thenReturn(requestsReceivedCounter);
        when(pluginMetrics.counter(SplunkHecService.REQUESTS_SUCCESS_TOTAL)).thenReturn(requestsSuccessCounter);
        when(pluginMetrics.counter(SplunkHecService.REQUESTS_FAILED_TOTAL)).thenReturn(requestsFailedCounter);
        when(pluginMetrics.counter(SplunkHecService.REQUESTS_AUTH_FAILED_TOTAL)).thenReturn(requestsAuthFailedCounter);
        when(pluginMetrics.counter(SplunkHecService.EVENTS_RECEIVED_TOTAL)).thenReturn(eventsReceivedCounter);
        when(pluginMetrics.counter(SplunkHecService.EVENTS_WRITTEN_TOTAL)).thenReturn(eventsWrittenCounter);
        when(pluginMetrics.counter(SplunkHecService.BUFFER_FULL_TOTAL)).thenReturn(bufferFullCounter);
        when(pluginMetrics.counter(SplunkHecService.PARSE_ERRORS_TOTAL)).thenReturn(parseErrorsCounter);
        when(pluginMetrics.summary(SplunkHecService.REQUEST_SIZE_BYTES)).thenReturn(requestSizeSummary);
        when(pluginMetrics.summary(SplunkHecService.EVENTS_PER_REQUEST)).thenReturn(eventsPerRequestSummary);
        when(pluginMetrics.timer(SplunkHecService.REQUEST_LATENCY)).thenReturn(requestLatencyTimer);
        lenient().when(requestLatencyTimer.record(any(Supplier.class)))
                .thenAnswer(invocation -> {
                    Supplier<?> supplier = invocation.getArgument(0);
                    return supplier.get();
                });

        final HecTokenConfig tokenConfig = mock(HecTokenConfig.class);
        when(tokenConfig.getToken()).thenReturn(validToken);
        lenient().when(tokenConfig.isEnabled()).thenReturn(true);
        when(tokenConfig.getDefaults()).thenReturn(null);
        when(config.getTokens()).thenReturn(List.of(tokenConfig));
        when(config.isAcknowledgements()).thenReturn(false);
        when(config.isFlattenEvent()).thenReturn(true);
        when(config.getRawLineBreaker()).thenReturn("\n");
        when(config.getDefaultSourcetype()).thenReturn("httpevent");
        lenient().when(config.isWarnFutureTimestamps()).thenReturn(false);
        lenient().when(config.getAcknowledgementExpiry()).thenReturn(Duration.ofSeconds(300));

        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager, TEST_EVENT_FACTORY);
    }

    @Test
    void handleEvent_with_valid_single_event_returns_success() throws Exception {
        final String body = "{\"event\": \"test message\", \"host\": \"web01\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        assertThat(responseBody.get("text"), equalTo("Success"));
        verify(requestsSuccessCounter).increment();
    }

    @Test
    void handleEvent_with_missing_auth_returns_401_code_2() throws Exception {
        final String body = "{\"event\": \"test message\"}";
        final AggregatedHttpRequest request = createRequest(body, null);

        final AggregatedHttpResponse response = service.handleEvent(request).aggregate().join();
        final Map<String, Object> responseBody = parseAggregated(response);

        assertThat(response.status(), equalTo(HttpStatus.UNAUTHORIZED));
        assertThat(responseBody.get("code"), equalTo(2));
        verify(requestsAuthFailedCounter).increment();
    }

    @Test
    void handleEvent_with_unknown_token_returns_403_code_4() throws Exception {
        final String body = "{\"event\": \"test message\"}";
        final AggregatedHttpRequest request = createRequest(body, "Splunk invalid-token");

        final AggregatedHttpResponse response = service.handleEvent(request).aggregate().join();
        final Map<String, Object> responseBody = parseAggregated(response);

        assertThat(response.status(), equalTo(HttpStatus.FORBIDDEN));
        assertThat(responseBody.get("code"), equalTo(4));
        verify(requestsAuthFailedCounter).increment();
    }

    @Test
    void handleEvent_with_bearer_scheme_returns_401_code_3() throws Exception {
        final String body = "{\"event\": \"test message\"}";
        final AggregatedHttpRequest request = createRequest(body, "Bearer " + validToken);

        final AggregatedHttpResponse response = service.handleEvent(request).aggregate().join();
        final Map<String, Object> responseBody = parseAggregated(response);

        assertThat(response.status(), equalTo(HttpStatus.UNAUTHORIZED));
        assertThat(responseBody.get("code"), equalTo(3));
    }

    @Test
    void handleEvent_with_disabled_token_returns_403() throws Exception {
        final String disabledToken = "disabled-token-value";
        final HecTokenConfig disabledConfig = mock(HecTokenConfig.class);
        when(disabledConfig.getToken()).thenReturn(disabledToken);
        when(disabledConfig.isEnabled()).thenReturn(false);
        final SplunkHecSourceConfig disabledSourceConfig = mock(SplunkHecSourceConfig.class);
        when(disabledSourceConfig.getTokens()).thenReturn(List.of(disabledConfig));
        when(disabledSourceConfig.isAcknowledgements()).thenReturn(false);
        lenient().when(disabledSourceConfig.isFlattenEvent()).thenReturn(true);
        lenient().when(disabledSourceConfig.getRawLineBreaker()).thenReturn("\n");
        lenient().when(disabledSourceConfig.getDefaultSourcetype()).thenReturn("httpevent");
        lenient().when(disabledSourceConfig.getAcknowledgementExpiry()).thenReturn(Duration.ofSeconds(300));
        final SplunkHecService disabledService =
                new SplunkHecService(8000, buffer, pluginMetrics, disabledSourceConfig, acknowledgementSetManager, TEST_EVENT_FACTORY);

        final AggregatedHttpRequest request = createRequest("{\"event\": \"x\"}", "Splunk " + disabledToken);
        final HttpResponse response = disabledService.handleEvent(request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.FORBIDDEN));
        verify(requestsAuthFailedCounter).increment();
    }

    @Test
    void handleEvent_with_missing_event_field_returns_error_code_12() throws Exception {
        final String body = "{\"host\": \"web01\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(12));
        assertThat(responseBody.get("invalid-event-number"), equalTo(0));
        verify(requestsFailedCounter).increment();
    }

    @Test
    void handleEvent_with_non_numeric_time_falls_back_to_current_time_and_succeeds() throws Exception {
        final String body = "{\"event\": \"test message\", \"time\": \"not-a-number\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(requestsSuccessCounter).increment();
    }

    @Test
    void handleEvent_with_non_finite_time_falls_back_to_current_time_and_succeeds() throws Exception {
        final String body = "{\"event\": \"test message\", \"time\": \"1e309\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(requestsSuccessCounter).increment();
    }

    @Test
    void handleEvent_with_overflow_time_falls_back_to_current_time_and_succeeds() throws Exception {
        final String body = "{\"event\": \"test message\", \"time\": 9300000000000000000}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(requestsSuccessCounter).increment();
    }

    @Test
    void handleEvent_with_concatenated_events() throws Exception {
        final String body = "{\"event\": \"first\"}{\"event\": \"second\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(eventsReceivedCounter).increment(2);
    }

    @Test
    void handleEvent_with_malformed_json_returns_error_code_6() throws Exception {
        final String body = "{\"event\": broken json}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(6));
        assertThat(responseBody.get("invalid-event-number"), equalTo(0));
        verify(parseErrorsCounter).increment();
    }

    @Test
    void handleEvent_with_empty_body_returns_no_data() throws Exception {
        final AggregatedHttpRequest request = createRequest("", authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(5));
    }

    @Test
    void handleEvent_when_buffer_full_returns_503() throws Exception {
        doThrow(new TimeoutException("buffer full")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(bufferFullCounter).increment();
    }

    @Test
    void handleEvent_with_non_string_metadata_values_coerces_and_succeeds() throws Exception {
        final String body = "{\"event\": \"msg\", \"host\": 123, \"source\": true, \"sourcetype\": 4.5, \"index\": 9}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(requestsSuccessCounter).increment();
    }

    @Test
    void handleEvent_with_token_defaults_missing_sourcetype_falls_back_to_default() throws Exception {
        final HecTokenConfig tokenConfigWithDefaults = mock(HecTokenConfig.class);
        when(tokenConfigWithDefaults.getToken()).thenReturn(validToken);
        lenient().when(tokenConfigWithDefaults.isEnabled()).thenReturn(true);
        final HecTokenConfig.HecTokenDefaults defaults = mock(HecTokenConfig.HecTokenDefaults.class);
        lenient().when(defaults.getIndex()).thenReturn("idx");
        lenient().when(defaults.getSourcetype()).thenReturn(null);
        lenient().when(defaults.getSource()).thenReturn(null);
        lenient().when(defaults.getHost()).thenReturn(null);
        lenient().when(defaults.getFields()).thenReturn(Collections.emptyMap());
        when(tokenConfigWithDefaults.getDefaults()).thenReturn(defaults);
        when(config.getTokens()).thenReturn(List.of(tokenConfigWithDefaults));

        final SplunkHecService svc = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager, TEST_EVENT_FACTORY);
        final String body = "{\"event\": \"msg\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        svc.handleEvent(request);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Collection<Record<Event>>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(captor.capture(), anyInt());
        final Event written = captor.getValue().iterator().next().getData();
        assertThat(written.get("sourcetype", String.class), equalTo("httpevent"));
        assertThat(written.getMetadata().getAttribute(HecMetadataKeyAttributes.SOURCETYPE), equalTo("httpevent"));
    }

    @Test
    void handleEvent_with_object_event_flattens_when_enabled() throws Exception {
        final String body = "{\"event\": {\"method\": \"GET\", \"path\": \"/api\"}}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        service.handleEvent(request);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Collection<Record<Event>>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(captor.capture(), anyInt());
        final Event written = captor.getValue().iterator().next().getData();
        assertThat(written.get("method", String.class), equalTo("GET"));
        assertThat(written.get("path", String.class), equalTo("/api"));
    }

    @Test
    void handleRaw_with_valid_request_returns_success() throws Exception {
        final String body = "line1\nline2\nline3";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(request,
                "main", "syslog", "forwarder", "host1");
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(eventsReceivedCounter).increment(3);
    }

    @Test
    void handleRaw_with_empty_body_returns_no_data() throws Exception {
        final AggregatedHttpRequest request = createRequest("", authHeader);

        final HttpResponse response = service.handleRaw(request,
                null, null, null, null);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(5));
    }

    @Test
    void handleRaw_with_missing_auth_returns_401() throws Exception {
        final AggregatedHttpRequest request = createRequest("line1\nline2", null);

        final HttpResponse response = service.handleRaw(request,
                null, null, null, null);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    void handleHealth_returns_healthy_response() throws Exception {
        final HttpResponse response = service.handleHealth();
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(17));
        assertThat(responseBody.get("text"), equalTo("HEC is healthy"));
    }

    @Test
    void handleAck_when_disabled_returns_error_code_14() throws Exception {
        final String body = "{\"acks\": [1, 2, 3]}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(14));
    }

    @Test
    void handleEvent_with_acknowledgements_enabled_requires_channel_header() throws Exception {
        when(config.isAcknowledgements()).thenReturn(true);
        final Counter ackRequestsCounter = mock(Counter.class);
        final Counter ackConfirmedCounter = mock(Counter.class);
        final Counter ackExpiredCounter = mock(Counter.class);
        when(pluginMetrics.counter("ackRequestsTotal")).thenReturn(ackRequestsCounter);
        when(pluginMetrics.counter("ackConfirmedTotal")).thenReturn(ackConfirmedCounter);
        when(pluginMetrics.counter("ackExpiredTotal")).thenReturn(ackExpiredCounter);
        lenient().when(pluginMetrics.gauge(eq("ackPending"), any(), any())).thenReturn(null);

        final SplunkHecService ackService = new SplunkHecService(8000, buffer, pluginMetrics,
                config, acknowledgementSetManager, TEST_EVENT_FACTORY);

        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = ackService.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(10));
    }

    @Test
    void handleEvent_with_acknowledgements_and_channel_returns_ackId() throws Exception {
        when(config.isAcknowledgements()).thenReturn(true);
        final Counter ackRequestsCounter = mock(Counter.class);
        final Counter ackConfirmedCounter = mock(Counter.class);
        final Counter ackExpiredCounter = mock(Counter.class);
        when(pluginMetrics.counter("ackRequestsTotal")).thenReturn(ackRequestsCounter);
        when(pluginMetrics.counter("ackConfirmedTotal")).thenReturn(ackConfirmedCounter);
        when(pluginMetrics.counter("ackExpiredTotal")).thenReturn(ackExpiredCounter);
        lenient().when(pluginMetrics.gauge(eq("ackPending"), any(), any())).thenReturn(null);

        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenReturn(acknowledgementSet);

        final SplunkHecService ackService = new SplunkHecService(8000, buffer, pluginMetrics,
                config, acknowledgementSetManager, TEST_EVENT_FACTORY);

        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "channel-123");

        final HttpResponse response = ackService.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        assertThat(responseBody.get("ackId"), notNullValue());
    }

    @Test
    void handleEvent_second_event_missing_event_field_returns_correct_event_number() throws Exception {
        final String body = "{\"event\": \"good\"}{\"host\": \"bad\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(12));
        assertThat(responseBody.get("invalid-event-number"), equalTo(1));
    }

    @Test
    void handleEvent_with_timestamp_writes_to_buffer() throws Exception {
        final String body = "{\"event\": \"msg\", \"time\": 1713196800.5}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(buffer).writeAll(any(Collection.class), anyInt());
    }

    @Test
    void handleRaw_skips_empty_lines() throws Exception {
        final String body = "line1\n\n\nline2\n";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(request,
                null, null, null, null);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        verify(eventsReceivedCounter).increment(2);
    }

    @Test
    void handleRaw_with_token_defaults_applies_defaults() throws Exception {
        final HecTokenConfig tokenConfigWithDefaults = mock(HecTokenConfig.class);
        when(tokenConfigWithDefaults.getToken()).thenReturn(validToken);
        lenient().when(tokenConfigWithDefaults.isEnabled()).thenReturn(true);
        final HecTokenConfig.HecTokenDefaults defaults = mock(HecTokenConfig.HecTokenDefaults.class);
        lenient().when(defaults.getIndex()).thenReturn("default-idx");
        lenient().when(defaults.getSourcetype()).thenReturn("default-st");
        lenient().when(defaults.getSource()).thenReturn("default-src");
        lenient().when(defaults.getHost()).thenReturn("default-host");
        lenient().when(defaults.getFields()).thenReturn(Collections.emptyMap());
        when(tokenConfigWithDefaults.getDefaults()).thenReturn(defaults);
        when(config.getTokens()).thenReturn(List.of(tokenConfigWithDefaults));

        final SplunkHecService svc = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager, TEST_EVENT_FACTORY);
        final String body = "log line";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = svc.handleRaw(request,
                null, null, null, null);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_token_defaults_and_explicit_fields() throws Exception {
        final HecTokenConfig tokenConfigWithDefaults = mock(HecTokenConfig.class);
        when(tokenConfigWithDefaults.getToken()).thenReturn(validToken);
        lenient().when(tokenConfigWithDefaults.isEnabled()).thenReturn(true);
        final HecTokenConfig.HecTokenDefaults defaults = mock(HecTokenConfig.HecTokenDefaults.class);
        lenient().when(defaults.getIndex()).thenReturn("default-idx");
        lenient().when(defaults.getSourcetype()).thenReturn("default-st");
        lenient().when(defaults.getSource()).thenReturn("default-src");
        lenient().when(defaults.getHost()).thenReturn("default-host");
        lenient().when(defaults.getFields()).thenReturn(Map.of("default-key", "default-val"));
        when(tokenConfigWithDefaults.getDefaults()).thenReturn(defaults);
        when(config.getTokens()).thenReturn(List.of(tokenConfigWithDefaults));

        final SplunkHecService svc = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager, TEST_EVENT_FACTORY);
        final String body = "{\"event\": \"msg\", \"host\": \"explicit-host\", \"source\": \"explicit-src\", " +
                "\"sourcetype\": \"explicit-st\", \"index\": \"explicit-idx\", " +
                "\"fields\": {\"app\": \"myapp\"}}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = svc.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_no_time_uses_current_time() throws Exception {
        final String body = "{\"event\": \"no-time-event\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_generic_write_exception_returns_500() throws Exception {
        doThrow(new RuntimeException("internal error")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
    }

    @Test
    void handleRaw_generic_write_exception_returns_500() throws Exception {
        doThrow(new RuntimeException("internal error")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "some log line";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(request,
                null, null, null, null);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
    }

    @Test
    void handleRaw_buffer_full_returns_503() throws Exception {
        doThrow(new TimeoutException("timeout")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "some log line";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(request,
                null, null, null, null);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
    }

    @Test
    void handleRaw_with_all_query_params() throws Exception {
        final String body = "log line";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(request,
                "my-index", "my-sourcetype", "my-source", "my-host");
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleRaw_with_blank_query_params_uses_defaults() throws Exception {
        final String body = "log line";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(request,
                "  ", "  ", "  ", "  ");
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_blank_host_source_sourcetype_in_hec_event() throws Exception {
        final String body = "{\"event\": \"msg\", \"host\": \"\", \"source\": \"\", \"sourcetype\": \"\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_no_metadata_no_defaults() throws Exception {
        when(config.getDefaultSourcetype()).thenReturn(null);
        final SplunkHecService svc = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager, TEST_EVENT_FACTORY);

        final String body = "{\"event\": \"msg\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = svc.handleEvent(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void shutdown_leaves_service_responsive() throws Exception {
        service.shutdown();

        final HttpResponse response = service.handleHealth();
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(17));
    }

    @Test
    void handleAck_without_auth_is_rejected_before_the_acknowledgements_disabled_check() throws Exception {
        final AggregatedHttpRequest request = createRequest("{\"acks\": [0]}", null);

        final AggregatedHttpResponse response = service.handleAck(request).aggregate().join();
        final Map<String, Object> responseBody = parseAggregated(response);

        assertThat(response.status(), equalTo(HttpStatus.UNAUTHORIZED));
        assertThat(responseBody.get("code"), equalTo(2));
        verify(requestsAuthFailedCounter).increment();
    }

    @Test
    void handleAck_with_auth_and_acknowledgements_disabled_returns_ack_disabled() throws Exception {
        final AggregatedHttpRequest request = createRequest("{\"acks\": [0]}", authHeader);

        final AggregatedHttpResponse response = service.handleAck(request).aggregate().join();
        final Map<String, Object> responseBody = parseAggregated(response);

        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
        assertThat(responseBody.get("code"), equalTo(14));
    }

    private AggregatedHttpRequest createRequest(final String body, final String authHeader) {
        final RequestHeaders requestHeaders;
        if (authHeader != null) {
            requestHeaders = RequestHeaders.builder()
                    .method(HttpMethod.POST)
                    .path("/services/collector/event")
                    .add(HttpHeaderNames.AUTHORIZATION, authHeader)
                    .build();
        } else {
            requestHeaders = RequestHeaders.builder()
                    .method(HttpMethod.POST)
                    .path("/services/collector/event")
                    .build();
        }

        return AggregatedHttpRequest.of(requestHeaders,
                HttpData.of(StandardCharsets.UTF_8, body));
    }

    private AggregatedHttpRequest createRequestWithChannel(final String body, final String authHeader, final String channel) {
        final RequestHeaders requestHeaders = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .path("/services/collector/event")
                .add(HttpHeaderNames.AUTHORIZATION, authHeader)
                .add("X-Splunk-Request-Channel", channel)
                .build();

        return AggregatedHttpRequest.of(requestHeaders,
                HttpData.of(StandardCharsets.UTF_8, body));
    }

    private Map<String, Object> parseResponse(final HttpResponse response) throws Exception {
        final String content = response.aggregate().join().content().toStringUtf8();
        return OBJECT_MAPPER.readValue(content, new TypeReference<>() { });
    }

    private Map<String, Object> parseAggregated(final AggregatedHttpResponse response) throws Exception {
        return OBJECT_MAPPER.readValue(response.content().toStringUtf8(), new TypeReference<>() { });
    }
}
