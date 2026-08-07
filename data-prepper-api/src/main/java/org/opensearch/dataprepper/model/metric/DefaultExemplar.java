/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.metric;

import java.util.Map;

/**
 * The default implementation of {@link Exemplar}
 *
 * @since 1.4
 */
public class DefaultExemplar implements Exemplar {

    private String time;
    private Double value;
    private Map<String, Object> attributes;
    private String spanId;
    private String traceId;

    // required for serialization
    DefaultExemplar() {}

    public DefaultExemplar(String time, Double value, String spanId, String traceId, Map<String, Object> attributes) {
        this.time = time;
        this.value = value;
        this.spanId = spanId;
        this.traceId = traceId;
        this.attributes = attributes;
    }

    @Override
    public String getTime() {
        return time;
    }

    @Override
    public Double getValue() {
        return value;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public String getSpanId() {
        return spanId;
    }

    @Override
    public String getTraceId() {
        return traceId;
    }
}
