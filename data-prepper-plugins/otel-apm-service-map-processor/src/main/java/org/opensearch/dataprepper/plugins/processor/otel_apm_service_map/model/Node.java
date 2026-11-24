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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.ToIntFunction;

public class Node {

    @JsonProperty("type")
    private final String type;

    @JsonProperty("keyAttributes")
    private final KeyAttributes keyAttributes;

    @JsonProperty("groupByAttributes")
    private final Map<String, String> groupByAttributes;

    /**
     * Exact peer identity for synthesized dependency nodes, so callers can filter the dependency's
     * spans/logs precisely rather than parse the node name. Empty for service nodes.
     */
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

    // dependencyAttributes is descriptive metadata, NOT identity: it is deliberately excluded from
    // equals/hashCode. Including it would (a) shift every existing nodeConnectionHash on upgrade
    // (appending a field changes Objects.hash), and (b) split one logical node into several when a
    // descriptive value varies per call (e.g. producer vs consumer messaging.operation, or per-host
    // db attributes) — the opposite of the "shared node" this feature intends.
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(type, node.type) &&
                Objects.equals(keyAttributes, node.keyAttributes) &&
                Objects.equals(groupByAttributes, node.groupByAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, keyAttributes, groupByAttributes);
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

    /**
     * Node identity ({@code environment} + {@code name}) plus optional descriptive keys (the
     * platform and infrastructure keys of a service, e.g. {@code k8s.cluster.name}). Only
     * {@code environment} and {@code name} are identity: the descriptive keys are serialized flat
     * next to them but are deliberately excluded from equals/hashCode, so they never change a
     * {@code nodeConnectionHash} or split one logical node into several (the same pattern as
     * {@link Node#getDependencyAttributes()}).
     */
    public static class KeyAttributes {
        @JsonProperty("environment")
        private final String environment;

        @JsonProperty("name")
        private final String name;

        /** Descriptive keys; each holds the distinct values observed for this node (bounded by the caller). */
        private final Map<String, Set<String>> descriptiveAttributes = new TreeMap<>();

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

        /** Descriptive keys, serialized flat next to environment / name; values are arrays. */
        @JsonAnyGetter
        public Map<String, Set<String>> getDescriptiveAttributes() {
            return Collections.unmodifiableMap(descriptiveAttributes);
        }

        /**
         * Adds a descriptive value; a key holds at most {@code maxValues} distinct values.
         * Identity keys ({@code environment}, {@code name}) are ignored.
         */
        public void addDescriptiveAttribute(final String key, final String value, final int maxValues) {
            if (key == null || value == null || "environment".equals(key) || "name".equals(key)) {
                return;
            }
            final Set<String> values = descriptiveAttributes.computeIfAbsent(key, k -> new TreeSet<>());
            if (values.size() < maxValues || values.contains(value)) {
                values.add(value);
            }
        }

        /** Merges another node's descriptive values into this one, with per-key bounds. */
        public void mergeDescriptiveAttributes(final KeyAttributes other,
                                               final ToIntFunction<String> maxValues) {
            if (other == null || other == this) {
                return;
            }
            other.descriptiveAttributes.forEach((key, values) ->
                    values.forEach(value -> addDescriptiveAttribute(key, value, maxValues.applyAsInt(key))));
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
                    (descriptiveAttributes.isEmpty() ? "" : ", descriptive=" + descriptiveAttributes) +
                    '}';
        }
    }
}
