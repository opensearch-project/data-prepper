/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Quantile;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TSDBDocumentBuilderTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_TIME = "2024-02-02T10:30:00Z";
    private static final long TEST_TIMESTAMP_MILLIS = 1706869800000L;

    private TSDBDocumentBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new TSDBDocumentBuilder();
    }

    // --- Gauge Tests ---

    @Test
    void build_gauge_returns_single_document() throws Exception {
        final Map<String, Object> attributes = Map.of("host", "server-01");
        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("cpu_temp")
                .withValue(72.5)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("GAUGE")
                .build();

        final List<String> docs = builder.build(gauge);

        assertEquals(1, docs.size());
        final Map<String, Object> doc = parseJson(docs.get(0));
        assertEquals("__name__ cpu_temp host server-01", doc.get("labels"));
        assertEquals(TEST_TIMESTAMP_MILLIS, ((Number) doc.get("timestamp")).longValue());
        assertEquals(72.5, ((Number) doc.get("value")).doubleValue(), 0.001);
    }

    @Test
    void build_gauge_with_no_attributes() throws Exception {
        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("memory_usage")
                .withValue(85.0)
                .withTime(TEST_TIME)
                .withEventKind("GAUGE")
                .build();

        final List<String> docs = builder.build(gauge);

        assertEquals(1, docs.size());
        final Map<String, Object> doc = parseJson(docs.get(0));
        assertEquals("__name__ memory_usage", doc.get("labels"));
        assertEquals(85.0, ((Number) doc.get("value")).doubleValue(), 0.001);
    }

    // --- Sum Tests ---

    @Test
    void build_monotonic_sum_adds_total_suffix() throws Exception {
        final Map<String, Object> attributes = Map.of("method", "GET");
        final JacksonSum sum = JacksonSum.builder()
                .withName("http_requests")
                .withValue(100.0)
                .withIsMonotonic(true)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("SUM")
                .build();

        final List<String> docs = builder.build(sum);

        assertEquals(1, docs.size());
        final Map<String, Object> doc = parseJson(docs.get(0));
        assertEquals("__name__ http_requests_total method GET", doc.get("labels"));
        assertEquals(100.0, ((Number) doc.get("value")).doubleValue(), 0.001);
    }

    @Test
    void build_non_monotonic_sum_no_total_suffix() throws Exception {
        final JacksonSum sum = JacksonSum.builder()
                .withName("temperature")
                .withValue(22.5)
                .withIsMonotonic(false)
                .withTime(TEST_TIME)
                .withEventKind("SUM")
                .build();

        final List<String> docs = builder.build(sum);

        assertEquals(1, docs.size());
        final Map<String, Object> doc = parseJson(docs.get(0));
        assertEquals("__name__ temperature", doc.get("labels"));
    }

    @Test
    void build_monotonic_sum_already_has_total_suffix() throws Exception {
        final JacksonSum sum = JacksonSum.builder()
                .withName("http_requests_total")
                .withValue(100.0)
                .withIsMonotonic(true)
                .withTime(TEST_TIME)
                .withEventKind("SUM")
                .build();

        final List<String> docs = builder.build(sum);

        assertEquals(1, docs.size());
        final Map<String, Object> doc = parseJson(docs.get(0));
        // Should NOT double-append _total
        assertEquals("__name__ http_requests_total", doc.get("labels"));
    }

    // --- Histogram Tests ---

    @Test
    void build_histogram_returns_bucket_plus_count_plus_sum_documents() throws Exception {
        final Map<String, Object> attributes = Map.of("method", "GET");
        final JacksonHistogram histogram = JacksonHistogram.builder()
                .withName("request_duration")
                .withSum(5.5)
                .withCount(20L)
                .withBucketCountsList(List.of(5L, 5L, 5L, 5L))
                .withExplicitBoundsList(List.of(0.1, 0.5, 1.0))
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("HISTOGRAM")
                .build();

        final List<String> docs = builder.build(histogram);

        // 4 bucket docs + 1 _count + 1 _sum = 6
        assertEquals(6, docs.size());

        // Bucket 1: cumulative = 5
        final Map<String, Object> bucket1 = parseJson(docs.get(0));
        assertEquals("__name__ request_duration_bucket le 0.1 method GET", bucket1.get("labels"));
        assertEquals(5.0, ((Number) bucket1.get("value")).doubleValue(), 0.001);

        // Bucket 2: cumulative = 10
        final Map<String, Object> bucket2 = parseJson(docs.get(1));
        assertEquals("__name__ request_duration_bucket le 0.5 method GET", bucket2.get("labels"));
        assertEquals(10.0, ((Number) bucket2.get("value")).doubleValue(), 0.001);

        // Bucket 3: cumulative = 15
        final Map<String, Object> bucket3 = parseJson(docs.get(2));
        assertEquals("__name__ request_duration_bucket le 1 method GET", bucket3.get("labels"));
        assertEquals(15.0, ((Number) bucket3.get("value")).doubleValue(), 0.001);

        // Bucket 4 (+Inf): cumulative = 20
        final Map<String, Object> bucket4 = parseJson(docs.get(3));
        assertEquals("__name__ request_duration_bucket le +Inf method GET", bucket4.get("labels"));
        assertEquals(20.0, ((Number) bucket4.get("value")).doubleValue(), 0.001);

        // _count document
        final Map<String, Object> countDoc = parseJson(docs.get(4));
        assertEquals("__name__ request_duration_count method GET", countDoc.get("labels"));
        assertEquals(20.0, ((Number) countDoc.get("value")).doubleValue(), 0.001);

        // _sum document
        final Map<String, Object> sumDoc = parseJson(docs.get(5));
        assertEquals("__name__ request_duration_sum method GET", sumDoc.get("labels"));
        assertEquals(5.5, ((Number) sumDoc.get("value")).doubleValue(), 0.001);
    }

    @Test
    void build_histogram_all_documents_have_same_timestamp() throws Exception {
        final JacksonHistogram histogram = JacksonHistogram.builder()
                .withName("latency")
                .withSum(1.0)
                .withCount(10L)
                .withBucketCountsList(List.of(5L, 5L))
                .withExplicitBoundsList(List.of(0.5))
                .withTime(TEST_TIME)
                .withEventKind("HISTOGRAM")
                .build();

        final List<String> docs = builder.build(histogram);

        for (final String jsonDoc : docs) {
            final Map<String, Object> doc = parseJson(jsonDoc);
            assertEquals(TEST_TIMESTAMP_MILLIS, ((Number) doc.get("timestamp")).longValue());
        }
    }

    // --- Summary Tests ---

    @Test
    void build_summary_returns_quantile_plus_count_plus_sum_documents() throws Exception {
        final Map<String, Object> attributes = Map.of("service", "api");
        final List<Quantile> quantiles = Arrays.asList(
                new DefaultQuantile(0.5, 0.2),
                new DefaultQuantile(0.99, 0.8)
        );
        final JacksonSummary summary = JacksonSummary.builder()
                .withName("rpc_latency")
                .withQuantiles(quantiles)
                .withQuantilesValueCount(2)
                .withCount(1000L)
                .withSum(300.5)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("SUMMARY")
                .build();

        final List<String> docs = builder.build(summary);

        // 2 quantile docs + 1 _count + 1 _sum = 4
        assertEquals(4, docs.size());

        // Quantile 0.5
        final Map<String, Object> q1 = parseJson(docs.get(0));
        assertEquals("__name__ rpc_latency quantile 0.5 service api", q1.get("labels"));
        assertEquals(0.2, ((Number) q1.get("value")).doubleValue(), 0.001);

        // Quantile 0.99
        final Map<String, Object> q2 = parseJson(docs.get(1));
        assertEquals("__name__ rpc_latency quantile 0.99 service api", q2.get("labels"));
        assertEquals(0.8, ((Number) q2.get("value")).doubleValue(), 0.001);

        // _count
        final Map<String, Object> countDoc = parseJson(docs.get(2));
        assertEquals("__name__ rpc_latency_count service api", countDoc.get("labels"));
        assertEquals(1000.0, ((Number) countDoc.get("value")).doubleValue(), 0.001);

        // _sum
        final Map<String, Object> sumDoc = parseJson(docs.get(3));
        assertEquals("__name__ rpc_latency_sum service api", sumDoc.get("labels"));
        assertEquals(300.5, ((Number) sumDoc.get("value")).doubleValue(), 0.001);
    }

    // --- Label Sorting Tests ---

    @Test
    void labels_are_sorted_lexicographically() throws Exception {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("zone", "us-east");
        attributes.put("app", "myservice");
        attributes.put("host", "server-01");

        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("cpu_temp")
                .withValue(1.0)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("GAUGE")
                .build();

        final List<String> docs = builder.build(gauge);
        final Map<String, Object> doc = parseJson(docs.get(0));

        // Sorted: __name__, app, host, zone
        assertEquals("__name__ cpu_temp app myservice host server-01 zone us-east", doc.get("labels"));
    }

    // --- Label Sanitization Tests ---

    @Test
    void label_values_with_spaces_are_sanitized() throws Exception {
        final Map<String, Object> attributes = Map.of("handler", "/api/items with spaces");

        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("requests")
                .withValue(1.0)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("GAUGE")
                .build();

        final List<String> docs = builder.build(gauge);
        final Map<String, Object> doc = parseJson(docs.get(0));

        assertTrue(((String) doc.get("labels")).contains("handler /api/items_with_spaces"));
    }

    @Test
    void label_keys_with_invalid_chars_are_sanitized() throws Exception {
        // key "123" should become "_123" (digit-first gets underscore prepended)
        final Map<String, Object> attributes = Map.of("123", "val");

        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("test")
                .withValue(1.0)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("GAUGE")
                .build();

        final List<String> docs = builder.build(gauge);
        final Map<String, Object> doc = parseJson(docs.get(0));

        assertTrue(((String) doc.get("labels")).contains("_123 val"));
    }

    @Test
    void label_keys_with_dashes_are_sanitized() throws Exception {
        final Map<String, Object> attributes = Map.of("key-with-dash", "val");

        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("test")
                .withValue(1.0)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("GAUGE")
                .build();

        final List<String> docs = builder.build(gauge);
        final Map<String, Object> doc = parseJson(docs.get(0));

        assertTrue(((String) doc.get("labels")).contains("key_with_dash val"));
    }

    @Test
    void extra_label_is_merge_inserted_at_correct_sorted_position() throws Exception {
        // "le" sorts between __name__ and "method" (l < m)
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("method", "GET");
        attributes.put("region", "us-east");

        final JacksonHistogram histogram = JacksonHistogram.builder()
                .withName("duration")
                .withSum(1.0)
                .withCount(10L)
                .withBucketCountsList(List.of(10L))
                .withExplicitBoundsList(List.of(0.5))
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("HISTOGRAM")
                .build();

        final List<String> docs = builder.build(histogram);
        final Map<String, Object> bucketDoc = parseJson(docs.get(0));

        assertEquals("__name__ duration_bucket le 0.5 method GET region us-east", bucketDoc.get("labels"));
    }

    @Test
    void extra_label_sorting_after_all_attributes() throws Exception {
        // "quantile" sorts after "app" (q > a)
        final Map<String, Object> attributes = Map.of("app", "web");
        final List<Quantile> quantiles = Arrays.asList(new DefaultQuantile(0.99, 1.0));

        final JacksonSummary summary = JacksonSummary.builder()
                .withName("latency")
                .withQuantiles(quantiles)
                .withQuantilesValueCount(1)
                .withCount(1L)
                .withSum(1.0)
                .withTime(TEST_TIME)
                .withAttributes(attributes)
                .withEventKind("SUMMARY")
                .build();

        final List<String> docs = builder.build(summary);
        final Map<String, Object> quantileDoc = parseJson(docs.get(0));

        assertEquals("__name__ latency app web quantile 0.99", quantileDoc.get("labels"));
    }

    // --- Timestamp Tests ---

    @Test
    void build_with_valid_timestamp() throws Exception {
        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("test")
                .withValue(1.0)
                .withTime("2024-02-02T10:30:00Z")
                .withEventKind("GAUGE")
                .build();

        final List<String> docs = builder.build(gauge);
        final Map<String, Object> doc = parseJson(docs.get(0));

        assertEquals(TEST_TIMESTAMP_MILLIS, ((Number) doc.get("timestamp")).longValue());
    }

    // --- Non-Metric Event Test ---

    @Test
    void build_throws_for_non_metric_event() {
        final Event event = JacksonEvent.builder()
                .withEventType("LOG")
                .withData(Map.of("message", "hello"))
                .build();

        assertThrows(IllegalArgumentException.class, () -> builder.build(event));
    }

    private Map<String, Object> parseJson(final String json) throws Exception {
        return OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
    }
}
