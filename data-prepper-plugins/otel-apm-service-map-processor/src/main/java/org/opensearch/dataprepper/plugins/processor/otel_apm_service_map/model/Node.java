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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class Node {

    @JsonProperty("type")
    private final String type;

    @JsonProperty("keyAttributes")
    private final KeyAttributes keyAttributes;

    @JsonProperty("groupByAttributes")
    private final Map<String, String> groupByAttributes;

    // Exact peer identity for synthesized dependency nodes, so callers can filter the
    // dependency's spans/logs precisely rather than parse the node name. Empty for service nodes.
    @JsonProperty("dependencyAttributes")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private final Map<String, String> dependencyAttributes;

    public Node(final String type, final KeyAttributes keyAttributes) {
        this(type, keyAttributes, Collections.emptyMap(), Collections.emptyMap());
    }

    public Node(final String type, final KeyAttributes keyAttributes, final Map<String, String> groupByAttributes) {
        this(type, keyAttributes, groupByAttributes, Collections.emptyMap());
    }

    public Node(final String type,
                final KeyAttributes keyAttributes,
                final Map<String, String> groupByAttributes,
                final Map<String, String> dependencyAttributes) {
        this.type = type;
        this.keyAttributes = keyAttributes;
        this.groupByAttributes = groupByAttributes != null ? groupByAttributes : Collections.emptyMap();
        this.dependencyAttributes = dependencyAttributes != null ? dependencyAttributes : Collections.emptyMap();
    }

    public String getType() {
        return type;
    }

    public KeyAttributes getKeyAttributes() {
        return keyAttributes;
    }

    public Map<String, String> getGroupByAttributes() {
        return groupByAttributes;
    }

    public Map<String, String> getDependencyAttributes() {
        return dependencyAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(type, node.type) &&
                Objects.equals(keyAttributes, node.keyAttributes) &&
                Objects.equals(groupByAttributes, node.groupByAttributes) &&
                Objects.equals(dependencyAttributes, node.dependencyAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, keyAttributes, groupByAttributes, dependencyAttributes);
    }

    @Override
    public String toString() {
        return "Node{" +
                "type='" + type + '\'' +
                ", keyAttributes=" + keyAttributes +
                ", groupByAttributes=" + groupByAttributes +
                ", dependencyAttributes=" + dependencyAttributes +
                '}';
    }

    public static class KeyAttributes {
        @JsonProperty("environment")
        private final String environment;

        @JsonProperty("name")
        private final String name;

        public KeyAttributes(final String environment, final String name) {
            this.environment = environment;
            this.name = name;
        }

        public String getEnvironment() {
            return environment;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            KeyAttributes that = (KeyAttributes) o;
            return Objects.equals(environment, that.environment) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(environment, name);
        }

        @Override
        public String toString() {
            return "KeyAttributes{" +
                    "environment='" + environment + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
