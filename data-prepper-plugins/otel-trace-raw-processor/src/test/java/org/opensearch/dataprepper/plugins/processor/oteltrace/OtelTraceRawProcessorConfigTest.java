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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OtelTraceRawProcessorConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static OtelTraceRawProcessorConfig deserialize(final Map<String, Object> settings) {
        return OBJECT_MAPPER.convertValue(settings, OtelTraceRawProcessorConfig.class);
    }

    @Test
    void defaults_enableGenAiEnrichmentWithAiRootPropagation_andAreValid() {
        final OtelTraceRawProcessorConfig config = new OtelTraceRawProcessorConfig();

        assertThat(config.getTraceFlushIntervalSeconds(), equalTo(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC));
        assertThat(config.getTraceGroupCacheTimeToLive(),
                equalTo(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC)));
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
    void getTraceGroupCacheTimeToLive_whenUnset_defaultsToTheTraceFlushInterval() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of());

        assertThat(config.getTraceGroupCacheTimeToLive(),
                equalTo(Duration.ofSeconds(OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC)));
    }

    @Test
    void getTraceGroupCacheTimeToLive_whenUnset_defaultsToACustomizedTraceFlushInterval() {
        final long traceFlushIntervalSeconds = ThreadLocalRandom.current().nextLong(1, 10_000);
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("trace_flush_interval", traceFlushIntervalSeconds));

        assertThat(config.getTraceGroupCacheTimeToLive(), equalTo(Duration.ofSeconds(traceFlushIntervalSeconds)));
    }

    @Test
    void getTraceGroupCacheTimeToLive_whenConfigured_returnsTheConfiguredValue() {
        final long traceGroupCacheTimeToLiveSeconds = ThreadLocalRandom.current().nextLong(1, 10_000);
        final OtelTraceRawProcessorConfig config = deserialize(Map.of(
                "trace_flush_interval", 300,
                "trace_group_cache_ttl", Duration.ofSeconds(traceGroupCacheTimeToLiveSeconds).toString()));

        assertThat(config.getTraceGroupCacheTimeToLive(), equalTo(Duration.ofSeconds(traceGroupCacheTimeToLiveSeconds)));
    }

    @Test
    void genAiRootEnrichmentSettings_haveTheirDefaults_andAreValid() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of());

        assertThat(config.getGenAiRootEnrichmentCacheMaxSize(), equalTo(500_000L));
        assertThat(config.getGenAiRootHold(), equalTo(Duration.ZERO));
        assertThat(config.getGenAiRootHoldMaxSize(), equalTo(10_000L));
        assertThat(config.isGenAiRootEnrichmentSettingsValid(), is(true));
        assertThat(config.isGenAiRootHoldValid(), is(true));
    }

    @Test
    void deserialize_genAiRootEnrichmentSettings_readsTheValues() {
        final long cacheMaxSize = ThreadLocalRandom.current().nextLong(1, 1_000_000);
        final long holdSeconds = ThreadLocalRandom.current().nextLong(1, OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC + 1);
        final long holdMaxSize = ThreadLocalRandom.current().nextLong(1, 100_000);
        final OtelTraceRawProcessorConfig config = deserialize(Map.of(
                "genai_root_enrichment_cache_max_size", cacheMaxSize,
                "genai_root_hold", Duration.ofSeconds(holdSeconds).toString(),
                "genai_root_hold_max_size", holdMaxSize));

        assertThat(config.getGenAiRootEnrichmentCacheMaxSize(), equalTo(cacheMaxSize));
        assertThat(config.getGenAiRootHold(), equalTo(Duration.ofSeconds(holdSeconds)));
        assertThat(config.getGenAiRootHoldMaxSize(), equalTo(holdMaxSize));
        assertThat(config.isGenAiRootHoldValid(), is(true));
    }

    @ParameterizedTest
    @CsvSource({
            "180, PT180S, true",
            "180, PT181S, false",
            "60, PT1M, true",
            "60, PT61S, false",
            "180, PT0S, true",
            "180, PT-1S, false"
    })
    void isGenAiRootHoldValid_isTrueOnlyFromZeroToTheTraceFlushInterval(final long traceFlushInterval, final String hold,
                                                                        final boolean valid) {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of(
                "trace_flush_interval", traceFlushInterval, "genai_root_hold", hold));

        assertThat(config.isGenAiRootHoldValid(), is(valid));
    }

    @Test
    void isGenAiRootHoldValid_hasAnAssertTrueConstraint() throws NoSuchMethodException {
        final AssertTrue assertTrue = OtelTraceRawProcessorConfig.class.getDeclaredMethod("isGenAiRootHoldValid")
                .getAnnotation(AssertTrue.class);

        assertThat(assertTrue, notNullValue());
        assertThat(assertTrue.message(), containsString("trace_flush_interval"));
    }

    @Test
    void genAiRootHoldMaxSize_mustBeAtLeastOne() throws NoSuchFieldException {
        final Min min = OtelTraceRawProcessorConfig.class.getDeclaredField("genAiRootHoldMaxSize").getAnnotation(Min.class);

        assertThat(min, notNullValue());
        assertThat(min.value(), equalTo(1L));
    }

    @Test
    void deserialize_genAiRootHoldOfZero_disablesHolding() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_root_hold", "PT0S"));

        assertThat(config.getGenAiRootHold(), equalTo(Duration.ZERO));
    }

    @ParameterizedTest
    @CsvSource({
            "genai_root_enrichment_cache_max_size, 1000",
            "genai_root_hold, PT10S",
            "genai_root_hold_max_size, 500"
    })
    void isGenAiRootEnrichmentSettingsValid_whenASettingIsSetWithEnrichmentDisabled_isFalse(final String setting, final String value) {
        final Object parsed = setting.endsWith("_max_size") ? (Object) Long.valueOf(value) : value;
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("disable_genai_enrichment", true, setting, parsed));

        assertThat(config.isGenAiRootEnrichmentSettingsValid(), is(false));
    }

    @Test
    void isGenAiRootEnrichmentSettingsValid_whenSetWithEnrichmentEnabled_isTrue() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of(
                "genai_root_enrichment_cache_max_size", 1000,
                "genai_root_hold", "PT10S",
                "genai_root_hold_max_size", 500));

        assertThat(config.isGenAiRootEnrichmentSettingsValid(), is(true));
    }

    @Test
    void isGenAiRootEnrichmentSettingsValid_hasAnAssertTrueConstraint() throws NoSuchMethodException {
        final AssertTrue assertTrue = OtelTraceRawProcessorConfig.class.getDeclaredMethod("isGenAiRootEnrichmentSettingsValid")
                .getAnnotation(AssertTrue.class);

        assertThat(assertTrue, notNullValue());
        assertThat(assertTrue.message(), containsString("disable_genai_enrichment"));
    }

    @Test
    void genAiRootEnrichmentCacheMaxSize_mustBeAtLeastOne() throws NoSuchFieldException {
        final Min min = OtelTraceRawProcessorConfig.class.getDeclaredField("genAiRootEnrichmentCacheMaxSize").getAnnotation(Min.class);

        assertThat(min, notNullValue());
        assertThat(min.value(), equalTo(1L));
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
            "genai_root_marking, outermost",
            "genai_root_marking, none",
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
    void getGenAiRootMarking_whenNotSet_isOutermost() {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of());

        assertThat(config.getGenAiRootMarking(), equalTo(GenAiRootMarking.OUTERMOST));
    }

    @ParameterizedTest
    @CsvSource({
            "outermost, OUTERMOST",
            "agent_spans, AGENT_SPANS",
            "none, NONE"
    })
    void deserialize_genAiRootMarking_readsEachOption(final String option, final GenAiRootMarking expected) {
        final OtelTraceRawProcessorConfig config = deserialize(Map.of("genai_root_marking", option));

        assertThat(config.getGenAiRootMarking(), equalTo(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {"AGENT_SPANS", "Outermost", "agent-spans", "all"})
    void deserialize_genAiRootMarkingNotWrittenExactly_failsWithTheAllowedValues(final String option) {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> deserialize(Map.of("genai_root_marking", option)));

        assertThat(exception.getMessage(), containsString("outermost, agent_spans, none"));
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

    @Test
    void environmentSource_defaultsToPlatformFirst_andReadsDeploymentFirst() {
        assertThat(new OtelTraceRawProcessorConfig().getEnvironmentSource(),
                equalTo(org.opensearch.dataprepper.plugins.otel.common.EnvironmentSource.PLATFORM_FIRST));
        assertThat(deserialize(Map.of("environment_source", "deployment_first")).getEnvironmentSource(),
                equalTo(org.opensearch.dataprepper.plugins.otel.common.EnvironmentSource.DEPLOYMENT_FIRST));
    }
}
