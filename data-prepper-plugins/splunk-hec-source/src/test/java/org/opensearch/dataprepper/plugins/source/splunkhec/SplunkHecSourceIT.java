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
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.awaitility.Awaitility;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SplunkHecSourceIT {

    private static final EventFactory TEST_EVENT_FACTORY = TestEventFactory.getTestEventFactory();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TOKEN = "test-hec-token-12345";
    private static final int PORT = 18088;
    private static final String BASE_URL = "http://127.0.0.1:" + PORT;

    private SplunkHecSource source;
    private Buffer<Record<Event>> buffer;
    private PluginMetrics pluginMetrics;
    private List<Record<Event>> writtenRecords;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws Exception {
        pluginMetrics = PluginMetrics.fromNames("splunk_hec", "test-pipeline");
        writtenRecords = Collections.synchronizedList(new ArrayList<>());

        buffer = mock(Buffer.class);
        doAnswer(invocation -> {
            Collection<Record<Event>> records = invocation.getArgument(0);
            writtenRecords.addAll(records);
            return null;
        }).when(buffer).writeAll(any(Collection.class), anyInt());

        final SplunkHecSourceConfig config = mock(SplunkHecSourceConfig.class);
        when(config.getPort()).thenReturn(PORT);
        when(config.getPath()).thenReturn("/services/collector");
        when(config.getAuthentication()).thenReturn(null);
        when(config.isSsl()).thenReturn(false);
        when(config.getMaxConnectionCount()).thenReturn(500);
        when(config.getRequestTimeoutInMillis()).thenReturn(10000);
        when(config.getBufferTimeoutInMillis()).thenReturn(8000);
        when(config.getThreadCount()).thenReturn(200);
        when(config.getMaxPendingRequests()).thenReturn(1024);
        when(config.hasHealthCheckService()).thenReturn(false);
        when(config.isAcknowledgements()).thenReturn(false);
        when(config.isFlattenEvent()).thenReturn(true);
        when(config.getRawLineBreaker()).thenReturn("\n");
        when(config.getDefaultSourcetype()).thenReturn("httpevent");
        lenient().when(config.isWarnFutureTimestamps()).thenReturn(false);
        lenient().when(config.getMaxRequestLength()).thenReturn(null);

        final HecTokenConfig tokenConfig =
                mock(HecTokenConfig.class);
        when(tokenConfig.getToken()).thenReturn(TOKEN);
        lenient().when(tokenConfig.isEnabled()).thenReturn(true);
        when(tokenConfig.getDefaults()).thenReturn(null);
        when(config.getTokens()).thenReturn(List.of(tokenConfig));

        final PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");

        final PluginFactory pluginFactory = mock(PluginFactory.class);
        final AcknowledgementSetManager ackManager = mock(AcknowledgementSetManager.class);

        source = new SplunkHecSource(config, pluginMetrics, pluginFactory, pipelineDescription, ackManager, TEST_EVENT_FACTORY);
        source.start(buffer);
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.stop();
        }
    }

    @Test
    void event_endpoint_accepts_single_event() throws Exception {
        final String body = "{\"event\": \"test message\", \"host\": \"web01\"}";
        final AggregatedHttpResponse response = sendEvent(body);

        assertThat(response.status(), equalTo(HttpStatus.OK));
        final Map<String, Object> responseBody = parseResponse(response);
        assertThat(responseBody.get("code"), equalTo(0));
        assertThat(responseBody.get("text"), equalTo("Success"));

        Awaitility.await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(writtenRecords, hasSize(1)));
        final Event event = writtenRecords.get(0).getData();
        assertThat(event.get("message", String.class), equalTo("test message"));
        assertThat(event.get("host", String.class), equalTo("web01"));
    }

    @Test
    void event_endpoint_accepts_concatenated_events() throws Exception {
        final String body = "{\"event\": \"first\"}{\"event\": \"second\"}{\"event\": \"third\"}";
        final AggregatedHttpResponse response = sendEvent(body);

        assertThat(response.status(), equalTo(HttpStatus.OK));

        Awaitility.await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(writtenRecords, hasSize(3)));
    }

    @Test
    void event_endpoint_rejects_missing_auth() throws Exception {
        final WebClient client = WebClient.of(BASE_URL);
        final RequestHeaders headers = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:" + PORT)
                .method(HttpMethod.POST)
                .path("/services/collector/event")
                .contentType(MediaType.JSON)
                .build();

        final AggregatedHttpResponse response = client.execute(headers, "{\"event\": \"test\"}")
                .aggregate().join();

        assertThat(response.status(), equalTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    void event_endpoint_rejects_unknown_token() throws Exception {
        final WebClient client = WebClient.of(BASE_URL);
        final RequestHeaders headers = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:" + PORT)
                .method(HttpMethod.POST)
                .path("/services/collector/event")
                .contentType(MediaType.JSON)
                .add(HttpHeaderNames.AUTHORIZATION, "Splunk invalid-token")
                .build();

        final AggregatedHttpResponse response = client.execute(headers, "{\"event\": \"test\"}")
                .aggregate().join();

        assertThat(response.status(), equalTo(HttpStatus.FORBIDDEN));
        final Map<String, Object> responseBody = parseResponse(response);
        assertThat(responseBody.get("code"), equalTo(4));
    }

    @Test
    void event_endpoint_rejects_missing_event_field() throws Exception {
        final String body = "{\"host\": \"web01\"}";
        final AggregatedHttpResponse response = sendEvent(body);

        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
        final Map<String, Object> responseBody = parseResponse(response);
        assertThat(responseBody.get("code"), equalTo(12));
        assertThat(responseBody.get("invalid-event-number"), equalTo(0));
    }

    @Test
    void event_endpoint_rejects_malformed_json() throws Exception {
        final String body = "{broken";
        final AggregatedHttpResponse response = sendEvent(body);

        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
        final Map<String, Object> responseBody = parseResponse(response);
        assertThat(responseBody.get("code"), equalTo(6));
    }

    @Test
    void raw_endpoint_accepts_multiline_text() throws Exception {
        final String body = "line1\nline2\nline3";
        final AggregatedHttpResponse response = sendRaw(body);

        assertThat(response.status(), equalTo(HttpStatus.OK));
        final Map<String, Object> responseBody = parseResponse(response);
        assertThat(responseBody.get("code"), equalTo(0));

        Awaitility.await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(writtenRecords.size(), equalTo(3)));
    }

    @Test
    void health_endpoint_returns_healthy() throws Exception {
        final WebClient client = WebClient.of(BASE_URL);
        final RequestHeaders headers = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:" + PORT)
                .method(HttpMethod.GET)
                .path("/services/collector/health")
                .build();

        final AggregatedHttpResponse response = client.execute(headers).aggregate().join();

        assertThat(response.status(), equalTo(HttpStatus.OK));
        final Map<String, Object> responseBody = parseResponse(response);
        assertThat(responseBody.get("code"), equalTo(17));
        assertThat(responseBody.get("text"), equalTo("HEC is healthy"));
    }

    @Test
    void event_endpoint_with_timestamp_and_metadata() throws Exception {
        final String body = "{\"event\": {\"method\": \"GET\"}, \"time\": 1713196800, \"host\": \"h1\", \"source\": \"s1\", \"sourcetype\": \"st1\", \"index\": \"idx1\"}";
        final AggregatedHttpResponse response = sendEvent(body);

        assertThat(response.status(), equalTo(HttpStatus.OK));
        Awaitility.await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(writtenRecords, hasSize(1)));
        final Event event = writtenRecords.get(0).getData();
        assertThat(event.get("method", String.class), equalTo("GET"));
        assertThat(event.get("host", String.class), equalTo("h1"));
    }

    private AggregatedHttpResponse sendEvent(final String body) {
        final WebClient client = WebClient.of(BASE_URL);
        final RequestHeaders headers = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:" + PORT)
                .method(HttpMethod.POST)
                .path("/services/collector/event")
                .contentType(MediaType.JSON)
                .add(HttpHeaderNames.AUTHORIZATION, "Splunk " + TOKEN)
                .build();
        return client.execute(headers, body).aggregate().join();
    }

    private AggregatedHttpResponse sendRaw(final String body) {
        final WebClient client = WebClient.of(BASE_URL);
        final RequestHeaders headers = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:" + PORT)
                .method(HttpMethod.POST)
                .path("/services/collector/raw")
                .contentType(MediaType.PLAIN_TEXT)
                .add(HttpHeaderNames.AUTHORIZATION, "Splunk " + TOKEN)
                .build();
        return client.execute(headers, body).aggregate().join();
    }

    private Map<String, Object> parseResponse(final AggregatedHttpResponse response) throws Exception {
        return OBJECT_MAPPER.readValue(response.content().toStringUtf8(), new TypeReference<>() { });
    }
}
