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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class PrometheusMetricUtilsTest {

    @Test
    void extractServiceName_returns_service_name_label() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("service.name", "my-service");
        attributes.put("job", "fallback");
        assertThat(PrometheusMetricUtils.extractServiceName(attributes), equalTo("my-service"));
    }

    @Test
    void extractServiceName_returns_service_name_underscore_label() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("service_name", "my-service");
        attributes.put("job", "fallback");
        assertThat(PrometheusMetricUtils.extractServiceName(attributes), equalTo("my-service"));
    }

    @Test
    void extractServiceName_returns_job_label_as_fallback() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("job", "my-job");
        assertThat(PrometheusMetricUtils.extractServiceName(attributes), equalTo("my-job"));
    }

    @Test
    void extractServiceName_returns_empty_when_no_match() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("instance", "localhost:9090");
        assertThat(PrometheusMetricUtils.extractServiceName(attributes), equalTo(""));
    }

    @ParameterizedTest
    @CsvSource({
            "http_requests_total, http_requests",
            "process_cpu_seconds_total, process_cpu_seconds",
            "some_metric_created, some_metric",
            "gauge_metric, gauge_metric"
    })
    void stripCounterSuffix_removes_expected_suffix(final String input, final String expected) {
        assertThat(PrometheusMetricUtils.stripCounterSuffix(input), equalTo(expected));
    }

    @Test
    void parseLeValue_returns_null_for_null_input() {
        assertThat(PrometheusMetricUtils.parseLeValue(null), nullValue());
    }

    @Test
    void parseLeValue_returns_positive_infinity() {
        assertThat(PrometheusMetricUtils.parseLeValue("+Inf"), equalTo(Double.POSITIVE_INFINITY));
    }

    @Test
    void parseLeValue_returns_negative_infinity() {
        assertThat(PrometheusMetricUtils.parseLeValue("-Inf"), equalTo(Double.NEGATIVE_INFINITY));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.5", "1.0", "10.0", "100"})
    void parseLeValue_returns_parsed_double(final String leValue) {
        assertThat(PrometheusMetricUtils.parseLeValue(leValue), equalTo(Double.parseDouble(leValue)));
    }

    @Test
    void parseLeValue_returns_null_for_unparseable() {
        assertThat(PrometheusMetricUtils.parseLeValue("notanumber"), nullValue());
    }

    @Test
    void parseQuantileValue_returns_null_for_null_input() {
        assertThat(PrometheusMetricUtils.parseQuantileValue(null), nullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.5", "0.9", "0.99", "1.0"})
    void parseQuantileValue_returns_parsed_double(final String quantileValue) {
        assertThat(PrometheusMetricUtils.parseQuantileValue(quantileValue), equalTo(Double.parseDouble(quantileValue)));
    }

    @Test
    void parseQuantileValue_returns_null_for_unparseable() {
        assertThat(PrometheusMetricUtils.parseQuantileValue("bad"), nullValue());
    }

    @Test
    void aggregation_temporality_constant_has_expected_value() {
        assertThat(PrometheusMetricUtils.AGGREGATION_TEMPORALITY_CUMULATIVE,
                equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));
    }
}
