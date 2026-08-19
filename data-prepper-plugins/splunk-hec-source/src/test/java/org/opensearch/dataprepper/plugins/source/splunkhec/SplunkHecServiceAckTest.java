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
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SplunkHecServiceAckTest {

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
    private ServiceRequestContext serviceRequestContext;
    @Mock
    private AcknowledgementSet acknowledgementSet;

    private SplunkHecService service;

    @BeforeEach
    void setUp() {
        validToken = UUID.randomUUID().toString();
        authHeader = "Splunk " + validToken;

        final Counter counter = mock(Counter.class);
        final DistributionSummary summary = mock(DistributionSummary.class);
        final Timer timer = mock(Timer.class);

        lenient().when(pluginMetrics.counter(any(String.class))).thenReturn(counter);
        lenient().when(pluginMetrics.summary(any(String.class))).thenReturn(summary);
        lenient().when(pluginMetrics.timer(any(String.class))).thenReturn(timer);
        lenient().when(pluginMetrics.gauge(any(String.class), any(AtomicLong.class), any(ToDoubleFunction.class)))
                .thenReturn(new AtomicLong(0));
        lenient().when(timer.record(any(Supplier.class)))
                .thenAnswer(invocation -> {
                    Supplier<?> supplier = invocation.getArgument(0);
                    return supplier.get();
                });

        final HecTokenConfig tokenConfig = mock(HecTokenConfig.class);
        when(tokenConfig.getToken()).thenReturn(validToken);
        lenient().when(tokenConfig.isEnabled()).thenReturn(true);
        final HecTokenConfig.HecTokenDefaults defaults = mock(HecTokenConfig.HecTokenDefaults.class);
        lenient().when(defaults.getIndex()).thenReturn("default-index");
        lenient().when(defaults.getSourcetype()).thenReturn("default-sourcetype");
        lenient().when(defaults.getSource()).thenReturn("default-source");
        lenient().when(defaults.getHost()).thenReturn("default-host");
        lenient().when(defaults.getFields()).thenReturn(Map.of("env", "test"));
        when(tokenConfig.getDefaults()).thenReturn(defaults);
        when(config.getTokens()).thenReturn(List.of(tokenConfig));
        when(config.isAcknowledgements()).thenReturn(true);
        when(config.isFlattenEvent()).thenReturn(true);
        when(config.getRawLineBreaker()).thenReturn("\n");
        when(config.getDefaultSourcetype()).thenReturn("httpevent");
        lenient().when(config.isWarnFutureTimestamps()).thenReturn(true);
        when(config.getAckExpiry()).thenReturn(Duration.ofSeconds(300));

        lenient().when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenReturn(acknowledgementSet);

        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);
    }

    @Test
    void handleEvent_with_ack_and_blank_channel_returns_error() throws Exception {
        final String body = "{\"event\": \"test\"}";
        final RequestHeaders requestHeaders = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .path("/services/collector/event")
                .add(HttpHeaderNames.AUTHORIZATION, authHeader)
                .add("X-Splunk-Request-Channel", "   ")
                .build();
        final AggregatedHttpRequest request = AggregatedHttpRequest.of(requestHeaders,
                HttpData.of(StandardCharsets.UTF_8, body));

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(10));
    }

    @Test
    void handleRaw_with_ack_and_blank_channel_returns_error() throws Exception {
        final String body = "line1";
        final RequestHeaders requestHeaders = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .path("/services/collector/raw")
                .add(HttpHeaderNames.AUTHORIZATION, authHeader)
                .add("X-Splunk-Request-Channel", "   ")
                .build();
        final AggregatedHttpRequest request = AggregatedHttpRequest.of(requestHeaders,
                HttpData.of(StandardCharsets.UTF_8, body));

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                null, null, null, null);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(10));
    }

    @Test
    void handleAck_with_blank_channel_returns_error() throws Exception {
        final String ackBody = "{\"acks\": [0]}";
        final RequestHeaders requestHeaders = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .path("/services/collector/ack")
                .add(HttpHeaderNames.AUTHORIZATION, authHeader)
                .add("X-Splunk-Request-Channel", "   ")
                .build();
        final AggregatedHttpRequest request = AggregatedHttpRequest.of(requestHeaders,
                HttpData.of(StandardCharsets.UTF_8, ackBody));

        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(10));
    }

    @Test
    void handleEvent_with_ack_and_channel_returns_ackId() throws Exception {
        final String body = "{\"event\": \"test message\", \"host\": \"web01\", \"source\": \"app.log\", \"sourcetype\": \"json\", \"index\": \"main\"}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-1");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        assertThat(responseBody.get("ackId"), notNullValue());
    }

    @Test
    void handleEvent_with_ack_and_object_event() throws Exception {
        final String body = "{\"event\": {\"method\": \"GET\", \"path\": \"/api\"}, \"time\": 1713196800}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-2");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_ack_and_string_time() throws Exception {
        final String body = "{\"event\": \"msg\", \"time\": \"1713196800.5\"}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-3");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_ack_and_future_timestamp_warns() throws Exception {
        final long futureTime = System.currentTimeMillis() / 1000 + 7200;
        final String body = "{\"event\": \"msg\", \"time\": " + futureTime + "}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-4");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_ack_and_fields() throws Exception {
        final String body = "{\"event\": \"msg\", \"fields\": {\"region\": \"us-east\"}}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-5");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_ack_buffer_full_returns_503() throws Exception {
        doThrow(new TimeoutException("buffer full")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-6");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(acknowledgementSet).cancel();
    }

    @Test
    void handleEvent_with_ack_generic_exception_returns_500() throws Exception {
        doThrow(new RuntimeException("unexpected")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-7");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
        verify(acknowledgementSet).cancel();
    }

    @Test
    void handleRaw_with_ack_and_channel_returns_ackId() throws Exception {
        final String body = "line1\nline2";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-8");

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                "web-logs", "access", "forwarder", "host1");
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
        assertThat(responseBody.get("ackId"), notNullValue());
    }

    @Test
    void handleRaw_with_ack_buffer_full_returns_503() throws Exception {
        doThrow(new TimeoutException("buffer full")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "line1\nline2";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-9");

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                null, null, null, null);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(acknowledgementSet).cancel();
    }

    @Test
    void handleRaw_with_ack_generic_exception_returns_500() throws Exception {
        doThrow(new RuntimeException("unexpected")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "line1";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-10");

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                null, null, null, null);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
        verify(acknowledgementSet).cancel();
    }

    @Test
    void handleRaw_with_ack_missing_channel_returns_error() throws Exception {
        final String body = "line1";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                null, null, null, null);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(10));
    }

    @Test
    void handleAck_with_valid_channel_and_ackIds() throws Exception {
        final AggregatedHttpRequest createRequest = createRequestWithChannel(
                "{\"event\": \"test\"}", authHeader, "ch-ack");
        service.handleEvent(serviceRequestContext, createRequest);

        final String ackBody = "{\"acks\": [0]}";
        final AggregatedHttpRequest request = createAckRequest(ackBody, authHeader, "ch-ack");
        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        @SuppressWarnings("unchecked")
        final Map<String, Object> acks = (Map<String, Object>) responseBody.get("acks");
        assertThat(acks.get("0"), equalTo(false));
    }

    @Test
    void handleAck_with_missing_auth_returns_401() throws Exception {
        final String ackBody = "{\"acks\": [0]}";
        final AggregatedHttpRequest request = createAckRequest(ackBody, null, "ch-1");
        final HttpResponse response = service.handleAck(request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    void handleAck_with_missing_channel_returns_400() throws Exception {
        final String ackBody = "{\"acks\": [0]}";
        final AggregatedHttpRequest request = createRequest(ackBody, authHeader);
        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(10));
    }

    @Test
    void handleAck_with_malformed_body_returns_error() throws Exception {
        final String ackBody = "not json";
        final AggregatedHttpRequest request = createAckRequest(ackBody, authHeader, "ch-1");
        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(6));
    }

    @Test
    void handleAck_with_missing_acks_field_returns_error() throws Exception {
        final String ackBody = "{\"other\": \"field\"}";
        final AggregatedHttpRequest request = createAckRequest(ackBody, authHeader, "ch-1");
        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(6));
    }

    @Test
    void handleEvent_with_array_event_value() throws Exception {
        final String body = "{\"event\": [1, 2, 3]}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-11");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_numeric_event_value() throws Exception {
        final String body = "{\"event\": 42}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-12");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_flatten_false_keeps_nested_event() throws Exception {
        when(config.isFlattenEvent()).thenReturn(false);
        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        final String body = "{\"event\": {\"method\": \"GET\"}}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-13");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_with_null_event_field_returns_error() throws Exception {
        final String body = "{\"event\": null}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-14");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(12));
    }

    @Test
    void handleRaw_with_only_empty_lines_returns_no_data() throws Exception {
        final String body = "\n\n\n";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-15");

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                null, null, null, null);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(5));
    }

    @Test
    void handleEvent_without_ack_generic_exception_returns_500() throws Exception {
        when(config.isAcknowledgements()).thenReturn(false);
        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        doThrow(new RuntimeException("unexpected")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
    }

    @Test
    void handleRaw_without_ack_buffer_full_returns_503() throws Exception {
        when(config.isAcknowledgements()).thenReturn(false);
        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        doThrow(new TimeoutException("buffer full")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "line1";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                null, null, null, null);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
    }

    @Test
    void handleRaw_without_ack_generic_exception_returns_500() throws Exception {
        when(config.isAcknowledgements()).thenReturn(false);
        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        doThrow(new RuntimeException("unexpected")).when(buffer).writeAll(any(Collection.class), anyInt());
        final String body = "line1";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                null, null, null, null);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
    }

    @Test
    void handleRaw_without_ack_success() throws Exception {
        when(config.isAcknowledgements()).thenReturn(false);
        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        final String body = "line1\nline2";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleRaw(serviceRequestContext, request,
                "idx", "st", "src", "h");
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void handleEvent_without_token_defaults_uses_default_sourcetype() throws Exception {
        final HecTokenConfig tokenConfigNoDefaults = mock(HecTokenConfig.class);
        when(tokenConfigNoDefaults.getToken()).thenReturn(validToken);
        lenient().when(tokenConfigNoDefaults.isEnabled()).thenReturn(true);
        when(tokenConfigNoDefaults.getDefaults()).thenReturn(null);
        when(config.getTokens()).thenReturn(List.of(tokenConfigNoDefaults));
        when(config.isAcknowledgements()).thenReturn(false);
        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequest(body, authHeader);

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(0));
    }

    @Test
    void shutdown_with_ack_manager_leaves_service_responsive() throws Exception {
        service.shutdown();

        final HttpResponse response = service.handleHealth();
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(17));
    }

    @Test
    void handleAck_with_json_null_body_returns_error() throws Exception {
        final String ackBody = "null";
        final AggregatedHttpRequest request = createAckRequest(ackBody, authHeader, "ch-1");
        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(6));
    }

    @Test
    void handleAck_with_non_array_acks_returns_error() throws Exception {
        final String ackBody = "{\"acks\": \"not-an-array\"}";
        final AggregatedHttpRequest request = createAckRequest(ackBody, authHeader, "ch-1");
        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(6));
    }

    @Test
    void handleAck_with_non_numeric_ack_element_returns_error() throws Exception {
        final String ackBody = "{\"acks\": [\"a\", \"b\"]}";
        final AggregatedHttpRequest request = createAckRequest(ackBody, authHeader, "ch-1");
        final HttpResponse response = service.handleAck(request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(6));
    }

    @Test
    void handleEvent_with_ack_malformed_json_returns_parse_error() throws Exception {
        final String body = "{broken json content";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-parse-err");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(6));
    }

    @Test
    void handleEvent_with_ack_empty_body_returns_no_data() throws Exception {
        final String body = "";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-empty");

        final HttpResponse response = service.handleEvent(serviceRequestContext, request);
        final Map<String, Object> responseBody = parseResponse(response);

        assertThat(responseBody.get("code"), equalTo(5));
    }

    @Test
    void acknowledgement_callback_with_true_confirms_ack() throws Exception {
        final AtomicReference<Consumer<Boolean>> callbackRef =
                new AtomicReference<>();
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    callbackRef.set(invocation.getArgument(0));
                    return acknowledgementSet;
                });

        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-callback");
        final Map<String, Object> createBody = parseResponse(service.handleEvent(serviceRequestContext, request));

        assertThat(callbackRef.get(), notNullValue());
        callbackRef.get().accept(true);

        final Object ackId = createBody.get("ackId");
        final AggregatedHttpRequest ackRequest = createAckRequest("{\"acks\": [" + ackId + "]}", authHeader, "ch-callback");
        @SuppressWarnings("unchecked")
        final Map<String, Object> acks = (Map<String, Object>) parseResponse(service.handleAck(ackRequest)).get("acks");
        assertThat(acks.get(String.valueOf(ackId)), equalTo(true));
    }

    @Test
    void acknowledgement_callback_with_false_does_not_confirm_ack() throws Exception {
        final AtomicReference<Consumer<Boolean>> callbackRef =
                new AtomicReference<>();
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    callbackRef.set(invocation.getArgument(0));
                    return acknowledgementSet;
                });

        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        final String body = "{\"event\": \"test\"}";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-callback-false");
        final Map<String, Object> createBody = parseResponse(service.handleEvent(serviceRequestContext, request));

        assertThat(callbackRef.get(), notNullValue());
        callbackRef.get().accept(false);

        final Object ackId = createBody.get("ackId");
        final AggregatedHttpRequest ackRequest =
                createAckRequest("{\"acks\": [" + ackId + "]}", authHeader, "ch-callback-false");
        @SuppressWarnings("unchecked")
        final Map<String, Object> acks = (Map<String, Object>) parseResponse(service.handleAck(ackRequest)).get("acks");
        assertThat(acks.get(String.valueOf(ackId)), equalTo(false));
    }

    @Test
    void handleRaw_acknowledgement_callback_with_true_confirms_ack() throws Exception {
        final AtomicReference<Consumer<Boolean>> callbackRef =
                new AtomicReference<>();
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    callbackRef.set(invocation.getArgument(0));
                    return acknowledgementSet;
                });

        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        final String body = "raw line";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-raw-cb");
        final Map<String, Object> createBody =
                parseResponse(service.handleRaw(serviceRequestContext, request, null, null, null, null));

        assertThat(callbackRef.get(), notNullValue());
        callbackRef.get().accept(true);

        final Object ackId = createBody.get("ackId");
        final AggregatedHttpRequest ackRequest = createAckRequest("{\"acks\": [" + ackId + "]}", authHeader, "ch-raw-cb");
        @SuppressWarnings("unchecked")
        final Map<String, Object> acks = (Map<String, Object>) parseResponse(service.handleAck(ackRequest)).get("acks");
        assertThat(acks.get(String.valueOf(ackId)), equalTo(true));
    }

    @Test
    void handleRaw_acknowledgement_callback_with_false_does_not_confirm() throws Exception {
        final AtomicReference<Consumer<Boolean>> callbackRef =
                new AtomicReference<>();
        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    callbackRef.set(invocation.getArgument(0));
                    return acknowledgementSet;
                });

        service = new SplunkHecService(8000, buffer, pluginMetrics, config, acknowledgementSetManager);

        final String body = "raw line";
        final AggregatedHttpRequest request = createRequestWithChannel(body, authHeader, "ch-raw-cb-f");
        final Map<String, Object> createBody =
                parseResponse(service.handleRaw(serviceRequestContext, request, null, null, null, null));

        assertThat(callbackRef.get(), notNullValue());
        callbackRef.get().accept(false);

        final Object ackId = createBody.get("ackId");
        final AggregatedHttpRequest ackRequest =
                createAckRequest("{\"acks\": [" + ackId + "]}", authHeader, "ch-raw-cb-f");
        @SuppressWarnings("unchecked")
        final Map<String, Object> acks = (Map<String, Object>) parseResponse(service.handleAck(ackRequest)).get("acks");
        assertThat(acks.get(String.valueOf(ackId)), equalTo(false));
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
        return AggregatedHttpRequest.of(requestHeaders, HttpData.of(StandardCharsets.UTF_8, body));
    }

    private AggregatedHttpRequest createRequestWithChannel(final String body, final String authHeader, final String channel) {
        final RequestHeaders requestHeaders = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .path("/services/collector/event")
                .add(HttpHeaderNames.AUTHORIZATION, authHeader)
                .add("X-Splunk-Request-Channel", channel)
                .build();
        return AggregatedHttpRequest.of(requestHeaders, HttpData.of(StandardCharsets.UTF_8, body));
    }

    private AggregatedHttpRequest createAckRequest(final String body, final String authHeader, final String channel) {
        final RequestHeadersBuilder builder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .path("/services/collector/ack");
        if (authHeader != null) {
            builder.add(HttpHeaderNames.AUTHORIZATION, authHeader);
        }
        if (channel != null) {
            builder.add("X-Splunk-Request-Channel", channel);
        }
        return AggregatedHttpRequest.of(builder.build(), HttpData.of(StandardCharsets.UTF_8, body));
    }

    private Map<String, Object> parseResponse(final HttpResponse response) throws Exception {
        final String content = response.aggregate().join().content().toStringUtf8();
        return OBJECT_MAPPER.readValue(content, new TypeReference<>() { });
    }
}
