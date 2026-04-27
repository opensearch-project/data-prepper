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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Attribute mappings from vendor-specific instrumentation libraries to OTel GenAI Semantic Conventions v1.39.0.
 * Mappings are loaded from {@code genai-attribute-mappings.yaml} in the jar resources.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/semconv/gen-ai/">OTel GenAI Semantic Conventions</a>
 */
final class GenAiAttributeMappings {

    private static final Logger LOG = LoggerFactory.getLogger(GenAiAttributeMappings.class);
    static final String MAPPINGS_FILE = "genai-attribute-mappings.yaml";

    static final class MappingTarget {
        private final String key;
        private final boolean wrapAsArray;

        MappingTarget(final String key, final boolean wrapAsArray) {
            this.key = key;
            this.wrapAsArray = wrapAsArray;
        }

        String getKey() {
            return key;
        }

        boolean isWrapAsArray() {
            return wrapAsArray;
        }
    }

    private static final Map<String, MappingTarget> LOOKUP_TABLE;
    private static final Map<String, String> OPERATION_NAME_VALUES;

    static {
        Map<String, MappingTarget> lookupTable = Collections.emptyMap();
        Map<String, String> operationNameValues = Collections.emptyMap();
        try (final InputStream is = GenAiAttributeMappings.class.getClassLoader().getResourceAsStream(MAPPINGS_FILE)) {
            if (is == null) {
                LOG.error("GenAI attribute mappings file not found: {}", MAPPINGS_FILE);
            } else {
                final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                @SuppressWarnings("unchecked")
                final Map<String, Object> yaml = mapper.readValue(is, Map.class);
                lookupTable = buildLookupTable(yaml);
                operationNameValues = buildOperationNameValues(yaml);
            }
        } catch (final IOException e) {
            LOG.error("Failed to load GenAI attribute mappings from {}", MAPPINGS_FILE, e);
        }
        LOOKUP_TABLE = Collections.unmodifiableMap(lookupTable);
        OPERATION_NAME_VALUES = Collections.unmodifiableMap(operationNameValues);
    }

    private GenAiAttributeMappings() {}

    /** Combined lookup table for all profiles. */
    static Map<String, MappingTarget> getLookupTable() {
        return LOOKUP_TABLE;
    }

    /** Value mappings for gen_ai.operation.name (case-insensitive keys). */
    static Map<String, String> getOperationNameValues() {
        return OPERATION_NAME_VALUES;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, MappingTarget> buildLookupTable(final Map<String, Object> yaml) {
        final Map<String, MappingTarget> table = new HashMap<>();
        for (final Map.Entry<String, Object> profile : yaml.entrySet()) {
            if (!(profile.getValue() instanceof List)) {
                continue;
            }
            for (final Object entry : (List<?>) profile.getValue()) {
                if (!(entry instanceof Map)) {
                    continue;
                }
                final Map<String, Object> mapping = (Map<String, Object>) entry;
                final String from = (String) mapping.get("from");
                final String to = (String) mapping.get("to");
                if (from == null || to == null) {
                    continue;
                }
                final boolean wrapSlice = Boolean.TRUE.equals(mapping.get("wrap_as_array"));
                table.putIfAbsent(from, new MappingTarget(to, wrapSlice));
            }
        }
        return table;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> buildOperationNameValues(final Map<String, Object> yaml) {
        final Object raw = yaml.get("operation_name_values");
        if (!(raw instanceof Map)) {
            return Collections.emptyMap();
        }
        final Map<String, String> values = new HashMap<>();
        for (final Map.Entry<String, Object> entry : ((Map<String, Object>) raw).entrySet()) {
            values.put(entry.getKey().toLowerCase(), (String) entry.getValue());
        }
        return values;
    }
}
