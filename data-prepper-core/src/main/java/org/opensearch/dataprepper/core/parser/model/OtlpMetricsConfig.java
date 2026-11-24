/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.registry.otlp.AggregationTemporality;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for the OTLP metric registry, read from the {@code otlp_metrics} section.
 */
public class OtlpMetricsConfig {
    static final String DEFAULT_ENDPOINT = "http://localhost:4318/v1/metrics";
    static final Duration DEFAULT_STEP = Duration.ofSeconds(60);

    private final String endpoint;
    private final AggregationTemporality aggregationTemporality;
    private final Duration step;
    private final Map<String, String> resourceAttributes;

    public OtlpMetricsConfig() {
        this(null, null, null, null);
    }

    @JsonCreator
    public OtlpMetricsConfig(
            @JsonProperty("endpoint") final String endpoint,
            @JsonProperty("aggregation_temporality") final String aggregationTemporality,
            @JsonProperty("step") final Duration step,
            @JsonProperty("resource_attributes") final Map<String, String> resourceAttributes) {
        this.endpoint = endpoint != null ? endpoint : DEFAULT_ENDPOINT;
        this.aggregationTemporality = aggregationTemporality != null
                ? parseAggregationTemporality(aggregationTemporality) : AggregationTemporality.DELTA;
        this.step = step != null ? step : DEFAULT_STEP;
        this.resourceAttributes = resourceAttributes != null ? resourceAttributes : Collections.emptyMap();
        validateEndpoint(this.endpoint);
        if (this.step.isZero() || this.step.isNegative()) {
            throw new IllegalArgumentException("otlp_metrics.step must be a positive duration, but was: " + this.step);
        }
        if (this.resourceAttributes.values().stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("otlp_metrics.resource_attributes values must not be empty.");
        }
    }

    public String getEndpoint() {
        return endpoint;
    }

    public AggregationTemporality getAggregationTemporality() {
        return aggregationTemporality;
    }

    public Duration getStep() {
        return step;
    }

    public Map<String, String> getResourceAttributes() {
        return resourceAttributes;
    }

    private static AggregationTemporality parseAggregationTemporality(final String value) {
        for (final AggregationTemporality temporality : AggregationTemporality.values()) {
            if (temporality.name().equalsIgnoreCase(value)) {
                return temporality;
            }
        }
        throw new IllegalArgumentException("otlp_metrics.aggregation_temporality must be delta or cumulative, but was: " + value);
    }

    private static void validateEndpoint(final String endpoint) {
        final URI uri;
        try {
            uri = URI.create(endpoint);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("otlp_metrics.endpoint is not a valid URL: " + endpoint, e);
        }
        if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getAuthority() == null) {
            throw new IllegalArgumentException("otlp_metrics.endpoint must be an http or https URL with a host, but was: " + endpoint);
        }
    }
}
