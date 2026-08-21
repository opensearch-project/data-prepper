/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.Collections;
import java.util.Map;

/**
 * Configuration for the CloudWatch Logs entity attached to PutLogEvents.
 */
public class EntityConfig {

    /**
     * Default bound on entities buffered concurrently in dynamic mode. A memory-safety backstop against
     * runaway key cardinality: each concurrent entity holds its own buffer, so this multiplied by
     * {@code max_request_size} is the worst-case buffered footprint.
     */
    public static final int DEFAULT_MAX_CARDINALITY = 1000;

    @JsonProperty("key_attributes")
    @NotEmpty
    private Map<String, String> keyAttributes = Collections.emptyMap();

    @JsonProperty("attributes")
    @NotNull
    private Map<String, String> attributes = Collections.emptyMap();

    @JsonProperty("max_cardinality")
    @Min(1)
    private int maxCardinality = DEFAULT_MAX_CARDINALITY;

    public Map<String, String> getKeyAttributes() {
        return Collections.unmodifiableMap(keyAttributes);
    }

    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * Maximum number of entities buffered at one time in dynamic mode. Beyond it, events collapse into a
     * shared fallback group carrying no per-resource entity. Ignored in static mode, where there is only
     * ever one group.
     */
    public int getMaxCardinality() {
        return maxCardinality;
    }

    /**
     * Dynamic when any {@code key_attributes}/{@code attributes} value is a Data Prepper format
     * expression carrying a {@code ${...}} field reference or expression; otherwise static (one
     * constant entity, original behavior).
     */
    public boolean isDynamic(final ExpressionEvaluator expressionEvaluator) {
        return containsTemplate(keyAttributes, expressionEvaluator)
                || containsTemplate(attributes, expressionEvaluator);
    }

    private static boolean containsTemplate(final Map<String, String> values,
                                            final ExpressionEvaluator expressionEvaluator) {
        if (values == null) {
            return false;
        }
        for (final String value : values.values()) {
            if (value == null) {
                continue;
            }
            if (!expressionEvaluator.extractDynamicKeysFromFormatExpression(value).isEmpty()
                    || !expressionEvaluator.extractDynamicExpressionsFromFormatExpression(value).isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
