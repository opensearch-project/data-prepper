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

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for the Prometheus scrape source.
 * Starts real Armeria HTTP servers that serve Prometheus text exposition format,
 * creates a real PrometheusScrapeService that scrapes them, and verifies
 * parsed metric events appear in a real BlockingBuffer.
 */
class PrometheusScrapeSourceIT {

    private static final int BUFFER_SIZE = 4096;
    private static final int BATCH_SIZE = 256;
    private static final int BUFFER_WRITE_TIMEOUT_MS = 5000;

    private final List<Server> servers = new ArrayList<>();
    private Buffer<Record<Event>> buffer;
    private PrometheusScrapeService scrapeService;

    @BeforeEach
    void setUp() {
        buffer = new BlockingBuffer<>(BUFFER_SIZE, BATCH_SIZE, "test-pipeline");
    }

    @AfterEach
    void tearDown() {
        if (scrapeService != null) {
            scrapeService.stop();
        }
        for (final Server server : servers) {
            server.stop().join();
        }
    }

    @Test
    void testScrapeGaugeMetric() throws Exception {
        final String metricsBody = "# TYPE cpu_temp gauge\n"
                + "cpu_temp{host=\"server1\"} 72.5 1706869800000\n";

        final Server server = startMetricsServer(metricsBody);
        startScrapeService(List.of(targetUrl(server)));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(greaterThanOrEqualTo(1)));

        final Event event = events.get(0);
        assertThat(event, instanceOf(Gauge.class));
        assertThat(event.get("kind", String.class), equalTo("GAUGE"));
        assertThat(event.get("name", String.class), equalTo("cpu_temp"));
        assertThat(event.get("value", Double.class), equalTo(72.5));
    }

    @Test
    void testScrapeCounterMetric() throws Exception {
        final String metricsBody = "# TYPE http_requests counter\n"
                + "http_requests_total{method=\"GET\"} 42 1706869800000\n";

        final Server server = startMetricsServer(metricsBody);
        startScrapeService(List.of(targetUrl(server)));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(greaterThanOrEqualTo(1)));

        final Event event = events.get(0);
        assertThat(event, instanceOf(Sum.class));
        assertThat(event.get("kind", String.class), equalTo("SUM"));
        assertThat(event.get("name", String.class), equalTo("http_requests"));
        assertThat(event.get("isMonotonic", Boolean.class), equalTo(true));
    }

    @Test
    void testScrapeHistogramMetric() throws Exception {
        final String metricsBody = "# TYPE req_duration histogram\n"
                + "req_duration_bucket{method=\"GET\",le=\"0.1\"} 5 1706869800000\n"
                + "req_duration_bucket{method=\"GET\",le=\"0.5\"} 10 1706869800000\n"
                + "req_duration_bucket{method=\"GET\",le=\"1.0\"} 15 1706869800000\n"
                + "req_duration_bucket{method=\"GET\",le=\"+Inf\"} 20 1706869800000\n"
                + "req_duration_count{method=\"GET\"} 20 1706869800000\n"
                + "req_duration_sum{method=\"GET\"} 5.5 1706869800000\n";

        final Server server = startMetricsServer(metricsBody);
        startScrapeService(List.of(targetUrl(server)));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(greaterThanOrEqualTo(1)));

        final Event event = events.get(0);
        assertThat(event, instanceOf(Histogram.class));
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
    }

    @Test
    void testScrapeSummaryMetric() throws Exception {
        final String metricsBody = "# TYPE rpc_latency summary\n"
                + "rpc_latency{service=\"api\",quantile=\"0.5\"} 0.2 1706869800000\n"
                + "rpc_latency{service=\"api\",quantile=\"0.99\"} 0.8 1706869800000\n"
                + "rpc_latency_count{service=\"api\"} 1000 1706869800000\n"
                + "rpc_latency_sum{service=\"api\"} 300.5 1706869800000\n";

        final Server server = startMetricsServer(metricsBody);
        startScrapeService(List.of(targetUrl(server)));

        final List<Event> events = drainBuffer();
        assertThat(events, hasSize(greaterThanOrEqualTo(1)));

        final Event event = events.get(0);
        assertThat(event, instanceOf(Summary.class));
        assertThat(event.get("kind", String.class), equalTo("SUMMARY"));
        assertThat(event.get("name", String.class), equalTo("rpc_latency"));
        assertThat(event.get("count", Long.class), equalTo(1000L));

        final List<?> quantiles = event.get("quantiles", List.class);
        assertThat(quantiles, hasSize(2));
    }

    @Test
    void testScrapeMultipleTargets() throws Exception {
        final String metricsBody1 = "# TYPE cpu_temp gauge\n"
                + "cpu_temp{host=\"server1\"} 72.5 1706869800000\n";
        final String metricsBody2 = "# TYPE memory_usage gauge\n"
                + "memory_usage{host=\"server2\"} 4096 1706869800000\n";

        final Server server1 = startMetricsServer(metricsBody1);
        final Server server2 = startMetricsServer(metricsBody2);
        startScrapeService(List.of(targetUrl(server1), targetUrl(server2)));

        final List<Event> events = drainBuffer(2);
        assertThat(events, hasSize(greaterThanOrEqualTo(2)));

        final long cpuTempCount = events.stream()
                .filter(e -> "cpu_temp".equals(e.get("name", String.class)))
                .count();
        final long memoryUsageCount = events.stream()
                .filter(e -> "memory_usage".equals(e.get("name", String.class)))
                .count();

        assertThat(cpuTempCount, is(greaterThanOrEqualTo(1L)));
        assertThat(memoryUsageCount, is(greaterThanOrEqualTo(1L)));
    }

    @Test
    void testScrapeNon2xxResponse() throws Exception {
        final Server server = Server.builder()
                .http(0)
                .service("/metrics", (ctx, req) ->
                        HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR))
                .build();
        server.start().join();
        servers.add(server);

        startScrapeService(List.of(targetUrl(server)));

        Thread.sleep(3000);

        final List<Event> events = drainBufferImmediate();
        assertThat(events, is(empty()));
    }

    private Server startMetricsServer(final String metricsBody) {
        final Server server = Server.builder()
                .http(0)
                .service("/metrics", (ctx, req) ->
                        HttpResponse.of(HttpStatus.OK, MediaType.PLAIN_TEXT_UTF_8, metricsBody))
                .build();
        server.start().join();
        servers.add(server);
        return server;
    }

    private String targetUrl(final Server server) {
        return "http://127.0.0.1:" + server.activeLocalPort() + "/metrics";
    }

    private void startScrapeService(final List<String> targetUrls) {
        final PrometheusScrapeConfig config = mock(PrometheusScrapeConfig.class);

        final List<ScrapeTargetConfig> targets = new ArrayList<>();
        for (final String url : targetUrls) {
            final ScrapeTargetConfig target = mock(ScrapeTargetConfig.class);
            when(target.getUrl()).thenReturn(url);
            targets.add(target);
        }

        when(config.getTargets()).thenReturn(targets);
        when(config.getScrapeInterval()).thenReturn(Duration.ofSeconds(1));
        when(config.getScrapeTimeout()).thenReturn(Duration.ofSeconds(5));
        when(config.isFlattenLabels()).thenReturn(false);
        when(config.isInsecure()).thenReturn(false);
        when(config.getAuthentication()).thenReturn(null);
        when(config.getSslCertificateFile()).thenReturn(null);

        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
        when(pluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));
        final Timer timer = mock(Timer.class);
        doAnswer(invocation -> {
            final Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(timer).record(org.mockito.ArgumentMatchers.<Runnable>any());
        when(pluginMetrics.timer(anyString())).thenReturn(timer);

        scrapeService = new PrometheusScrapeService(config, buffer, BUFFER_WRITE_TIMEOUT_MS, pluginMetrics);
        scrapeService.start();
    }

    private List<Event> drainBuffer() {
        return drainBuffer(1);
    }

    private List<Event> drainBuffer(final int minExpected) {
        final List<Event> events = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            final Collection<Record<Event>> records = buffer.read(100).getKey();
            for (final Record<Event> record : records) {
                events.add(record.getData());
            }
            if (events.size() >= minExpected) {
                return events;
            }
            if (records.isEmpty()) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return events;
    }

    private List<Event> drainBufferImmediate() {
        final List<Event> events = new ArrayList<>();
        final Collection<Record<Event>> records = buffer.read(100).getKey();
        for (final Record<Event> record : records) {
            events.add(record.getData());
        }
        return events;
    }
}
