/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Node;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

class ServicePlatformRegistryTest {

    private Map<String, Set<String>> lookup(final ServicePlatformRegistry registry, final String environment,
                                            final String name) {
        final Node.KeyAttributes keyAttributes = new Node.KeyAttributes(environment, name);
        registry.addTo(keyAttributes);
        return keyAttributes.getDescriptiveAttributes();
    }

    @Test
    void addTo_mergesTheValuesOfOneServiceOnly() {
        final ServicePlatformRegistry registry = new ServicePlatformRegistry(key -> 10);
        registry.record("prod", "pricing", Map.of("k8s.cluster.name", "a"));
        registry.record("prod", "pricing", Map.of("k8s.cluster.name", "b"));
        registry.record("prod", "billing", Map.of("k8s.cluster.name", "c"));
        registry.record("dev", "pricing", Map.of("k8s.cluster.name", "d"));

        assertThat(lookup(registry, "prod", "pricing"), equalTo(Map.of("k8s.cluster.name", Set.of("a", "b"))));
    }

    @Test
    void record_boundsTheValuesPerKey() {
        final ServicePlatformRegistry registry = new ServicePlatformRegistry(key -> "host.id".equals(key) ? 1 : 2);
        for (final String value : new String[]{"a", "b", "c"}) {
            registry.record(null, "pricing", Map.of("k8s.cluster.name", value, "host.id", value));
        }

        final Map<String, Set<String>> keys = lookup(registry, null, "pricing");
        assertThat(keys.get("k8s.cluster.name").size(), equalTo(2));
        assertThat(keys.get("host.id").size(), equalTo(1));
    }

    @Test
    void rotate_keepsThreeGenerationsThenDropsTheOldest() {
        final ServicePlatformRegistry registry = new ServicePlatformRegistry(key -> 10);
        registry.record("prod", "pricing", Map.of("k8s.cluster.name", "a"));
        registry.rotate();
        registry.rotate();
        assertThat(lookup(registry, "prod", "pricing"), equalTo(Map.of("k8s.cluster.name", Set.of("a"))));

        registry.rotate();
        assertThat(lookup(registry, "prod", "pricing"), anEmptyMap());
    }

    @Test
    void clear_dropsEveryGeneration() {
        final ServicePlatformRegistry registry = new ServicePlatformRegistry(key -> 10);
        registry.record("prod", "pricing", Map.of("k8s.cluster.name", "a"));
        registry.rotate();
        registry.clear();
        assertThat(lookup(registry, "prod", "pricing"), anEmptyMap());
    }
}
