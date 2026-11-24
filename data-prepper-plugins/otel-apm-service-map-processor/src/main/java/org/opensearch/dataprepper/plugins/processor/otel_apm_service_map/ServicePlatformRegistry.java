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

import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Node;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToIntFunction;

/**
 * The platform keys of each service ({@code environment} + {@code name}), held once per service rather
 * than on every span. Keys are kept in three generations that rotate with the span windows (spans are
 * recorded into the next window, and a window's nodes are built from the previous, current and next
 * windows), so a node gets the keys of every span that could have contributed to it. Each key holds at
 * most {@code maxValues.applyAsInt(key)} distinct values per service and generation.
 *
 * Recording is thread safe (process workers record concurrently); rotation and lookups happen on the
 * evaluating worker while the others wait at the window barrier.
 */
class ServicePlatformRegistry {
    private final ToIntFunction<String> maxValues;
    private volatile Map<Map.Entry<String, String>, Map<String, Set<String>>> previous = new ConcurrentHashMap<>();
    private volatile Map<Map.Entry<String, String>, Map<String, Set<String>>> current = new ConcurrentHashMap<>();
    private volatile Map<Map.Entry<String, String>, Map<String, Set<String>>> next = new ConcurrentHashMap<>();

    ServicePlatformRegistry(final ToIntFunction<String> maxValues) {
        this.maxValues = maxValues;
    }

    /** Records one span's platform keys for its service into the next generation. */
    void record(final String environment, final String serviceName, final Map<String, String> platformKeys) {
        if (platformKeys.isEmpty()) {
            return;
        }
        final Map<String, Set<String>> serviceKeys = next.computeIfAbsent(
                new SimpleImmutableEntry<>(environment, serviceName), k -> new ConcurrentHashMap<>());
        platformKeys.forEach((key, value) -> {
            final Set<String> values = serviceKeys.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
            if (!values.contains(value) && values.size() < maxValues.applyAsInt(key)) {
                values.add(value);
            }
        });
    }

    /** Adds the recorded keys of the service named by {@code keyAttributes}, bounded per key. */
    void addTo(final Node.KeyAttributes keyAttributes) {
        final Map.Entry<String, String> service =
                new SimpleImmutableEntry<>(keyAttributes.getEnvironment(), keyAttributes.getName());
        for (final Map<Map.Entry<String, String>, Map<String, Set<String>>> generation : List.of(previous, current, next)) {
            final Map<String, Set<String>> serviceKeys = generation.get(service);
            if (serviceKeys != null) {
                serviceKeys.forEach((key, values) -> values.forEach(value ->
                        keyAttributes.addDescriptiveAttribute(key, value, maxValues.applyAsInt(key))));
            }
        }
    }

    /** Drops the oldest generation, alongside the window rotation. */
    void rotate() {
        previous = current;
        current = next;
        next = new ConcurrentHashMap<>();
    }

    void clear() {
        previous = new ConcurrentHashMap<>();
        current = new ConcurrentHashMap<>();
        next = new ConcurrentHashMap<>();
    }
}
