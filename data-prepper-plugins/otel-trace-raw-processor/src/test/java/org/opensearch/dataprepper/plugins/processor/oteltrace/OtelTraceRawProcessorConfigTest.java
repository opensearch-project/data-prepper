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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OtelTraceRawProcessorConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static OtelTraceRawProcessorConfig deserialize(final Map<String, Object> settings) {
        return OBJECT_MAPPER.convertValue(settings, OtelTraceRawProcessorConfig.class);
    }

    @Test
    void defaults_enableGenAiEnrichmentWithAiRootPropagation_andAreValid() {
        final OtelTraceRawProcessorConfig config = new OtelTraceRawProcessorConfig();

        assertThat(config.getTraceFlushIntervalSeconds(), equalTo(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        assertThat(config.getTraceGroupCacheTimeToLive(), equalTo(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL));
        assertThat(config.getTraceGroupCacheMaxSize(), equalTo(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE));
        assertThat(config.isGenAiEnrichmentDisabled(), is(false));
        assertThat(config.getGenAiCoverageCacheMaxSize(), equalTo(500_000L));
        assertThat(config.getGenAiRootPropagation(), equalTo(GenAiRootPropagation.AI_ROOT));
        assertThat(config.isGenAiSettingsValid(), is(true));
    }

    @Test
    void deserialize_withNoGenAiSettings_usesTheDefaults() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of());

        assertThat(config.isGenAiEnrichmentDisabled(), is(false));
        assertThat(config.getGenAiCoverageCacheMaxSize(), equalTo(OtelTraceRawProcessorConfig.DEFAULT_GENAI_COVERAGE_CACHE_MAX_SIZE));
        assertThat(config.getGenAiRootPropagation(), equalTo(GenAiRootPropagation.AI_ROOT));
    }

    @Test
    void deserialize_disableGenAiEnrichmentTrue_disablesEnrichment() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("disable_genai_enrichment", true));

        assertThat(config.isGenAiEnrichmentDisabled(), is(true));
        assertThat(config.isGenAiSettingsValid(), is(true));
    }

    @ParameterizedTest
    @CsvSource({
            "genai_root_propagation, ai_root",
            "genai_root_propagation, none",
            "genai_coverage_cache_max_size, 1000"
    })
    void isGenAiSettingsValid_whenAGenAiSettingIsSetWithEnrichmentDisabled_isFalse(final String setting, final String value) {
        final Object parsed = setting.equals("genai_coverage_cache_max_size") ? (Object) Long.valueOf(value) : value;
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("disable_genai_enrichment", true, setting, parsed));

        assertThat(config.isGenAiSettingsValid(), is(false));
    }

    @Test
    void isGenAiSettingsValid_whenGenAiSettingsAreSetWithEnrichmentEnabled_isTrue() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of(
                "disable_genai_enrichment", false,
                "genai_root_propagation", "all_roots",
                "genai_coverage_cache_max_size", 1000));

        assertThat(config.isGenAiSettingsValid(), is(true));
    }

    @Test
    void isGenAiSettingsValid_hasAnAssertTrueConstraint() throws NoSuchMethodException {
        final AssertTrue assertTrue = OtelTraceRawProcessorConfig.class.getDeclaredMethod("isGenAiSettingsValid")
                .getAnnotation(AssertTrue.class);

        assertThat(assertTrue, notNullValue());
        assertThat(assertTrue.message(), containsString("disable_genai_enrichment"));
    }

    @ParameterizedTest
    @CsvSource({
            "ai_root, AI_ROOT",
            "all_roots, ALL_ROOTS",
            "none, NONE"
    })
    void deserialize_genAiRootPropagation_readsEachOption(final String option, final GenAiRootPropagation expected) {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_root_propagation", option));

        assertThat(config.getGenAiRootPropagation(), equalTo(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ALL_ROOTS", "None", "Ai_Root"})
    void deserialize_genAiRootPropagationInADifferentCase_fails(final String option) {
        assertThrows(IllegalArgumentException.class, () -> deserialize(Map.of("genai_root_propagation", option)));
    }

    @Test
    void deserialize_unknownGenAiRootPropagation_failsWithTheAllowedValues() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> deserialize(Map.of("genai_root_propagation", "roots")));

        assertThat(exception.getCause(), instanceOf(ValueInstantiationException.class));
        assertThat(exception.getMessage(), containsString("ai_root, all_roots, none"));
    }

    @Test
    void deserialize_genAiCoverageCacheMaxSize_readsTheValue() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_coverage_cache_max_size", 1234));

        assertThat(config.getGenAiCoverageCacheMaxSize(), equalTo(1234L));
    }

    @Test
    void genAiCoverageCacheMaxSize_mustBeAtLeastOne() throws NoSuchFieldException {
        final Field field = OtelTraceRawProcessorConfig.class.getDeclaredField("genAiCoverageCacheMaxSize");

        final Min min = field.getAnnotation(Min.class);

        assertThat(min, notNullValue());
        assertThat(min.value(), equalTo(1L));
    }
}
