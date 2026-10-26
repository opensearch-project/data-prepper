/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OtelTraceRawProcessorConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static OtelTraceRawProcessorConfig deserialize(final Map<String, Object> settings) {
        return OBJECT_MAPPER.convertValue(settings, OtelTraceRawProcessorConfig.class);
    }

    private static Set<ConstraintViolation<OtelTraceRawProcessorConfig>> validate(final OtelTraceRawProcessorConfig config) {
        final Validator validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();
        return validator.validate(config);
    }

    @Test
    void defaults_enableGenAiEnrichmentWithAiRootPropagation() {
        final OtelTraceRawProcessorConfig config = new OtelTraceRawProcessorConfig();

        assertThat(config.getTraceFlushIntervalSeconds(), equalTo(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        assertThat(config.getTraceGroupCacheTimeToLive(), equalTo(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL));
        assertThat(config.getTraceGroupCacheMaxSize(), equalTo(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE));
        assertThat(config.isGenAiEnrichmentEnabled(), is(true));
        assertThat(config.getGenAiCoverageCacheMaxSize(), equalTo(500_000L));
        assertThat(config.getGenAiRootPropagation(), equalTo(GenAiRootPropagation.AI_ROOT));
    }

    @Test
    void deserialize_withNoGenAiSettings_usesTheDefaults() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of());

        assertThat(config.isGenAiEnrichmentEnabled(), is(true));
        assertThat(config.getGenAiCoverageCacheMaxSize(), equalTo(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE));
        assertThat(config.getGenAiRootPropagation(), equalTo(GenAiRootPropagation.AI_ROOT));
    }

    @Test
    void deserialize_genAiEnrichmentEnabledFalse_disablesEnrichment() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_enrichment_enabled", false));

        assertThat(config.isGenAiEnrichmentEnabled(), is(false));
    }

    @ParameterizedTest
    @CsvSource({
            "ai_root, AI_ROOT",
            "all_roots, ALL_ROOTS",
            "none, NONE",
            "ALL_ROOTS, ALL_ROOTS",
            "None, NONE"
    })
    void deserialize_genAiRootPropagation_readsEachOption(final String option, final GenAiRootPropagation expected) {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_root_propagation", option));

        assertThat(config.getGenAiRootPropagation(), equalTo(expected));
    }

    @Test
    void deserialize_unknownGenAiRootPropagation_failsWithTheAllowedValues() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> deserialize(Map.of("genai_root_propagation", "roots")));

        assertThat(exception.getCause(), is(org.hamcrest.Matchers.instanceOf(ValueInstantiationException.class)));
        assertThat(exception.getMessage(), containsString("ai_root, all_roots, none"));
    }

    @Test
    void deserialize_genAiCoverageCacheMaxSize_readsTheValue() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_coverage_cache_max_size", 1234));

        assertThat(config.getGenAiCoverageCacheMaxSize(), equalTo(1234L));
    }

    @Test
    void validate_defaults_hasNoViolations() {
        assertThat(validate(new OtelTraceRawProcessorConfig()), is(empty()));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    void validate_genAiCoverageCacheMaxSizeBelowOne_isRejected(final long maxSize) {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_coverage_cache_max_size", maxSize));

        final Set<ConstraintViolation<OtelTraceRawProcessorConfig>> violations = validate(config);

        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo("genAiCoverageCacheMaxSize"));
    }

    @Test
    void validate_genAiCoverageCacheMaxSizeOfOne_isAccepted() {
        assertThat(validate(deserialize(Map.of("genai_coverage_cache_max_size", 1))), is(empty()));
    }
}
