/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class Service {

    @JsonProperty("keyAttributes")
    private final KeyAttributes keyAttributes;

    @JsonProperty("groupByAttributes")
    private final Map<String, String> groupByAttributes;

    public Service(final KeyAttributes keyAttributes) {
        this.keyAttributes = keyAttributes;
        this.groupByAttributes = Collections.emptyMap();
    }

    public Service(final KeyAttributes keyAttributes, final Map<String, String> groupByAttributes) {
        this.keyAttributes = keyAttributes;
        this.groupByAttributes = groupByAttributes != null ? groupByAttributes : Collections.emptyMap();
    }

    public KeyAttributes getKeyAttributes() {
        return keyAttributes;
    }

    public Map<String, String> getGroupByAttributes() {
        return groupByAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Service service = (Service) o;
        return Objects.equals(keyAttributes, service.keyAttributes) &&
                Objects.equals(groupByAttributes, service.groupByAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyAttributes, groupByAttributes);
    }

    @Override
    public String toString() {
        return "Service{" +
                "keyAttributes=" + keyAttributes +
                ", groupByAttributes=" + groupByAttributes +
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
