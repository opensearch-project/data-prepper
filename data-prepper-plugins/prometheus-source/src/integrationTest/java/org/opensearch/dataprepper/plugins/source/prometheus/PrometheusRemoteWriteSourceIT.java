/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.Server;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for the Prometheus Remote Write source.
 * Starts a real Armeria HTTP server with the PrometheusRemoteWriteService,
 * sends Snappy-compressed protobuf WriteRequests over HTTP,
 * and reads parsed metric events from a real BlockingBuffer.
 */
class PrometheusRemoteWriteSourceIT {

    private static final int BUFFER_SIZE = 4096;
    private static final int BATCH_SIZE = 256;
    private static final int BUFFER_TIMEOUT_MS = 5000;

    private Server server;
    private WebClient client;
    private Buffer<Record<Event>> buffer;

    @BeforeEach
    void setUp() throws Exception {
        buffer = new BlockingBuffer<>(BUFFER_SIZE, BATCH_SIZE, "test-pipeline");

        final PrometheusRemoteWriteSourceConfig config = mock(PrometheusRemoteWriteSourceConfig.class);
        when(config.isFlattenLabels()).thenReturn(false);
        final RemoteWriteProtobufParser protobufParser = new RemoteWriteProtobufParser(config);

        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
        when(pluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));
        when(pluginMetrics.summary(anyString())).thenReturn(mock(DistributionSummary.class));
        final Timer timer = mock(Timer.class);
        try {
            when(timer.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
                final Callable<?> callable = invocation.getArgument(0);
                return callable.call();
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        when(pluginMetrics.timer(anyString())).thenReturn(timer);

        final PrometheusRemoteWriteService service = new PrometheusRemoteWriteService(
                BUFFER_TIMEOUT_MS, buffer, pluginMetrics, protobufParser);

        server = Server.builder()
                .http(0)
                .annotatedService("/api/v1/write", service)
                .build();
        server.start().join();

        final int port = server.activeLocalPort();
        client = WebClient.of("http://127.0.0.1:" + port);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop().join();
        }
    }

    @Test
    void testGaugeEndToEnd() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(label("__name__", "cpu_temperature"))
                        .addLabels(label("host", "server-01"))
                        .addSamples(sample(72.5, 1706869800000L))
                        .build())
                .build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.OK));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(1));

        final Event event = events.get(0);
        assertThat(event.get("kind", String.class), equalTo("GAUGE"));
        assertThat(event.get("name", String.class), equalTo("cpu_temperature"));
        assertThat(event.get("value", Double.class), equalTo(72.5));
    }

    @Test
    void testCounterEndToEnd() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(label("__name__", "http_requests_total"))
                        .addLabels(label("method", "GET"))
                        .addSamples(sample(100.0, 1706869800000L))
                        .build())
                .build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.OK));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(1));

        final Event event = events.get(0);
        assertThat(event.get("kind", String.class), equalTo("SUM"));
        assertThat(event.get("name", String.class), equalTo("http_requests"));
        assertThat(event.get("isMonotonic", Boolean.class), equalTo(true));
    }

    @Test
    void testHistogramEndToEnd() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(bucketTimeSeries("req_duration", "0.1", 5, "method", "GET"))
                .addTimeseries(bucketTimeSeries("req_duration", "0.5", 10, "method", "GET"))
                .addTimeseries(bucketTimeSeries("req_duration", "1.0", 15, "method", "GET"))
                .addTimeseries(bucketTimeSeries("req_duration", "+Inf", 20, "method", "GET"))
                .addTimeseries(companionTimeSeries("req_duration_count", 20, "method", "GET"))
                .addTimeseries(companionTimeSeries("req_duration_sum", 5.5, "method", "GET"))
                .build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.OK));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(1));

        final Event event = events.get(0);
        assertThat(event.get("kind", String.class), equalTo("HISTOGRAM"));
        assertThat(event.get("name", String.class), equalTo("req_duration"));
        assertThat(event.get("count", Long.class), equalTo(20L));

        final List<Long> bucketCounts = event.get("bucketCountsList", List.class);
        assertThat(bucketCounts, hasSize(4));
        assertThat(bucketCounts.get(0), equalTo(5L));
        assertThat(bucketCounts.get(1), equalTo(5L));
        assertThat(bucketCounts.get(2), equalTo(5L));
        assertThat(bucketCounts.get(3), equalTo(5L));

        final List<Double> bounds = event.get("explicitBounds", List.class);
        assertThat(bounds, hasSize(3));

        @SuppressWarnings("unchecked")
        final Map<String, Object> attrs = event.get("attributes", Map.class);
        assertFalse(attrs.containsKey("le"));
        assertThat(attrs.get("method"), equalTo("GET"));
    }

    @Test
    void testSummaryEndToEnd() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(quantileTimeSeries("rpc_latency", "0.5", 0.2, "service", "api"))
                .addTimeseries(quantileTimeSeries("rpc_latency", "0.99", 0.8, "service", "api"))
                .addTimeseries(companionTimeSeries("rpc_latency_count", 1000, "service", "api"))
                .addTimeseries(companionTimeSeries("rpc_latency_sum", 300.5, "service", "api"))
                .build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.OK));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(1));

        final Event event = events.get(0);
        assertThat(event.get("kind", String.class), equalTo("SUMMARY"));
        assertThat(event.get("name", String.class), equalTo("rpc_latency"));
        assertThat(event.get("count", Long.class), equalTo(1000L));

        final List<?> quantiles = event.get("quantiles", List.class);
        assertThat(quantiles, hasSize(2));

        @SuppressWarnings("unchecked")
        final Map<String, Object> attrs = event.get("attributes", Map.class);
        assertFalse(attrs.containsKey("quantile"));
        assertThat(attrs.get("service"), equalTo("api"));
    }

    @Test
    void testMixedMetricTypesEndToEnd() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(label("__name__", "temperature"))
                        .addSamples(sample(72.5, 1706869800000L))
                        .build())
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(label("__name__", "errors_total"))
                        .addSamples(sample(5.0, 1706869800000L))
                        .build())
                .addTimeseries(bucketTimeSeries("latency", "1.0", 10))
                .addTimeseries(bucketTimeSeries("latency", "+Inf", 15))
                .addTimeseries(quantileTimeSeries("duration", "0.5", 0.1))
                .build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.OK));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(4));

        long gaugeCount = events.stream().filter(e -> "GAUGE".equals(e.get("kind", String.class))).count();
        long sumCount = events.stream().filter(e -> "SUM".equals(e.get("kind", String.class))).count();
        long histCount = events.stream().filter(e -> "HISTOGRAM".equals(e.get("kind", String.class))).count();
        long summaryCount = events.stream().filter(e -> "SUMMARY".equals(e.get("kind", String.class))).count();

        assertThat(gaugeCount, equalTo(1L));
        assertThat(sumCount, equalTo(1L));
        assertThat(histCount, equalTo(1L));
        assertThat(summaryCount, equalTo(1L));
    }

    @Test
    void testHistogramLabelSetGroupingEndToEnd() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(bucketTimeSeries("dur", "1.0", 10, "method", "GET"))
                .addTimeseries(bucketTimeSeries("dur", "+Inf", 20, "method", "GET"))
                .addTimeseries(bucketTimeSeries("dur", "1.0", 3, "method", "POST"))
                .addTimeseries(bucketTimeSeries("dur", "+Inf", 5, "method", "POST"))
                .build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.OK));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(2));

        for (final Event event : events) {
            assertThat(event.get("kind", String.class), equalTo("HISTOGRAM"));
        }
    }

    @Test
    void testServiceNameExtractedEndToEnd() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(label("__name__", "test_metric"))
                        .addLabels(label("job", "my-service"))
                        .addSamples(sample(1.0, 1706869800000L))
                        .build())
                .build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.OK));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(1));
        assertThat(events.get(0).get("serviceName", String.class), equalTo("my-service"));
    }

    @Test
    void testInvalidContentTypeReturns415() {
        final HttpRequest httpRequest = HttpRequest.of(
                RequestHeaders.builder(HttpMethod.POST, "/api/v1/write")
                        .contentType(MediaType.JSON)
                        .build(),
                HttpData.wrap("not protobuf".getBytes()));

        final AggregatedHttpResponse response = client.execute(httpRequest).aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE));
    }

    @Test
    void testEmptyBodyReturns400() {
        final HttpRequest httpRequest = HttpRequest.of(
                RequestHeaders.builder(HttpMethod.POST, "/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.empty());

        final AggregatedHttpResponse response = client.execute(httpRequest).aggregate().join();
        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void testCorruptedSnappyReturnsError() {
        final HttpRequest httpRequest = HttpRequest.of(
                RequestHeaders.builder(HttpMethod.POST, "/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.wrap("corrupted snappy data".getBytes()));

        final AggregatedHttpResponse response = client.execute(httpRequest).aggregate().join();
        assertThat(response.status().code(), greaterThanOrEqualTo(400));
    }

    @Test
    void testEmptyWriteRequestReturns204() throws Exception {
        final Remote.WriteRequest request = Remote.WriteRequest.newBuilder().build();

        final AggregatedHttpResponse response = sendWriteRequest(request);
        assertThat(response.status(), equalTo(HttpStatus.NO_CONTENT));
    }

    // Helper methods

    private AggregatedHttpResponse sendWriteRequest(final Remote.WriteRequest request) throws IOException {
        final byte[] compressed = Snappy.compress(request.toByteArray());
        final HttpRequest httpRequest = HttpRequest.of(
                RequestHeaders.builder(HttpMethod.POST, "/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.wrap(compressed));
        return client.execute(httpRequest).aggregate().join();
    }

    private List<Event> drainBuffer() {
        final List<Event> events = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            final Collection<Record<Event>> records = buffer.read(100).getKey();
            if (!records.isEmpty()) {
                for (final Record<Event> record : records) {
                    events.add(record.getData());
                }
                return events;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return events;
    }

    private static Types.Label.Builder label(final String name, final String value) {
        return Types.Label.newBuilder().setName(name).setValue(value);
    }

    private static Types.Sample.Builder sample(final double value, final long timestampMs) {
        return Types.Sample.newBuilder().setValue(value).setTimestamp(timestampMs);
    }

    private static Types.TimeSeries bucketTimeSeries(final String baseName, final String le, double value,
                                                      final String... extraLabels) {
        final Types.TimeSeries.Builder builder = Types.TimeSeries.newBuilder()
                .addLabels(label("__name__", baseName + "_bucket"))
                .addLabels(label("le", le))
                .addSamples(sample(value, 1706869800000L));
        for (int i = 0; i < extraLabels.length; i += 2) {
            builder.addLabels(label(extraLabels[i], extraLabels[i + 1]));
        }
        return builder.build();
    }

    private static Types.TimeSeries quantileTimeSeries(final String name, final String quantile, double value,
                                                        final String... extraLabels) {
        final Types.TimeSeries.Builder builder = Types.TimeSeries.newBuilder()
                .addLabels(label("__name__", name))
                .addLabels(label("quantile", quantile))
                .addSamples(sample(value, 1706869800000L));
        for (int i = 0; i < extraLabels.length; i += 2) {
            builder.addLabels(label(extraLabels[i], extraLabels[i + 1]));
        }
        return builder.build();
    }

    private static Types.TimeSeries companionTimeSeries(final String name, double value,
                                                         final String... extraLabels) {
        final Types.TimeSeries.Builder builder = Types.TimeSeries.newBuilder()
                .addLabels(label("__name__", name))
                .addSamples(sample(value, 1706869800000L));
        for (int i = 0; i < extraLabels.length; i += 2) {
            builder.addLabels(label(extraLabels[i], extraLabels[i + 1]));
        }
        return builder.build();
    }
}
