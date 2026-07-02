/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.opensearch.dataprepper.plugins.processor.oteltracegroup.ConnectionConfiguration.HOSTS;

class OTelTraceGroupProcessorConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_RAW_INDEX_ALIAS = "otel-v1-apm-span";
    private static final String INDICES_KEY = "indices";

    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");

    @Test
    void testDeserialize() {
        final Map<String, Object> pluginSetting = Map.of(HOSTS, TEST_HOSTS);
        final OTelTraceGroupProcessorConfig objectUnderTest = OBJECT_MAPPER.convertValue(
                pluginSetting, OTelTraceGroupProcessorConfig.class);
        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getEsConnectionConfig(), notNullValue());
        assertThat(objectUnderTest.getEsConnectionConfig().getHosts(), equalTo(TEST_HOSTS));
    }

    @Test
    void getIndices_returnsDefaultRawAliasWhenNotConfigured() {
        final Map<String, Object> pluginSetting = Map.of(HOSTS, TEST_HOSTS);
        final OTelTraceGroupProcessorConfig objectUnderTest = OBJECT_MAPPER.convertValue(
                pluginSetting, OTelTraceGroupProcessorConfig.class);
        assertThat(objectUnderTest.getIndices(), contains(DEFAULT_RAW_INDEX_ALIAS));
    }

    @Test
    void getIndices_returnsConfiguredIndicesWhenProvided() {
        final List<String> customIndices = List.of("my-traces-2026", "my-traces-2027");
        final Map<String, Object> pluginSetting = Map.of(HOSTS, TEST_HOSTS, INDICES_KEY, customIndices);
        final OTelTraceGroupProcessorConfig objectUnderTest = OBJECT_MAPPER.convertValue(
                pluginSetting, OTelTraceGroupProcessorConfig.class);
        assertThat(objectUnderTest.getIndices(), equalTo(customIndices));
    }

    @Test
    void getIndices_acceptsIndexPattern() {
        final List<String> patternIndices = List.of("otel-v1-apm-span-*");
        final Map<String, Object> pluginSetting = Map.of(HOSTS, TEST_HOSTS, INDICES_KEY, patternIndices);
        final OTelTraceGroupProcessorConfig objectUnderTest = OBJECT_MAPPER.convertValue(
                pluginSetting, OTelTraceGroupProcessorConfig.class);
        assertThat(objectUnderTest.getIndices(), equalTo(patternIndices));
    }
}
