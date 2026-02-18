/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class Operation {

    @JsonProperty("name")
    private final String name;

    @JsonProperty("attributes")
    private final Map<String, String> attributes;

    public Operation(final String name) {
        this.name = name;
        this.attributes = Collections.emptyMap();
    }

    public Operation(final String name, final Map<String, String> attributes) {
        this.name = name;
        this.attributes = attributes != null ? attributes : Collections.emptyMap();
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return Objects.equals(name, operation.name) && Objects.equals(attributes, operation.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, attributes);
    }

    @Override
    public String toString() {
        return "Operation{" +
                "name='" + name + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}
