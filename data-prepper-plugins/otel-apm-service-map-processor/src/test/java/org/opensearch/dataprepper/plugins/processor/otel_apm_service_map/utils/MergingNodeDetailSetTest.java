/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Node;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.NodeOperationDetail;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Operation;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MergingNodeDetailSetTest {
    private static final Instant T = Instant.parse("2026-10-30T00:00:00Z");

    private static Node.KeyAttributes keys(final String cluster, final String workload) {
        final Node.KeyAttributes k = new Node.KeyAttributes("prod", "checkout");
        k.addDescriptiveAttribute("k8s.cluster.name", cluster, 10);
        k.addDescriptiveAttribute("k8s.deployment.name", workload, 10);
        return k;
    }

    private static NodeOperationDetail leaf(final Node.KeyAttributes k) {
        return new NodeOperationDetail(new Node("service", k), null, new Operation("GET /"), null, T);
    }

    @Test
    void descriptive_keys_are_not_identity_so_hashes_do_not_move() {
        final Node.KeyAttributes plain = new Node.KeyAttributes("prod", "checkout");
        final Node.KeyAttributes withKeys = keys("a", "checkout");
        assertEquals(plain, withKeys);
        assertEquals(plain.hashCode(), withKeys.hashCode());
        assertEquals(leaf(plain).getNodeConnectionHash(), leaf(withKeys).getNodeConnectionHash());
    }

    @Test
    void duplicates_merge_their_values_instead_of_dropping_them() {
        final MergingNodeDetailSet set = new MergingNodeDetailSet(key -> 10);
        assertTrue(set.add(leaf(keys("a", "checkout"))));
        assertFalse(set.add(leaf(keys("b", "checkout"))));
        assertFalse(set.add(leaf(keys("a", "checkout-canary"))));
        assertEquals(1, set.size());
        final Map<String, Set<String>> merged =
                set.iterator().next().getSourceNode().getKeyAttributes().getDescriptiveAttributes();
        assertEquals(Set.of("a", "b"), merged.get("k8s.cluster.name"));
        assertEquals(Set.of("checkout", "checkout-canary"), merged.get("k8s.deployment.name"));
    }

    @Test
    void values_per_key_are_bounded() {
        final MergingNodeDetailSet set = new MergingNodeDetailSet(key -> 2);
        List.of("a", "b", "c").forEach(c -> set.add(leaf(keys(c, "w"))));
        assertEquals(2, set.iterator().next().getSourceNode().getKeyAttributes()
                .getDescriptiveAttributes().get("k8s.cluster.name").size());
    }

    @Test
    void identity_keys_cannot_be_overwritten_and_keys_serialize_flat() throws Exception {
        final Node.KeyAttributes k = keys("a", "checkout");
        k.addDescriptiveAttribute("name", "other", 10);
        k.addDescriptiveAttribute("environment", "other", 10);
        final Map<?, ?> json = new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(k), Map.class);
        assertEquals("checkout", json.get("name"));
        assertEquals("prod", json.get("environment"));
        assertEquals(List.of("a"), json.get("k8s.cluster.name"));
        assertEquals(List.of("checkout"), json.get("k8s.deployment.name"));
    }

    @Test
    void plain_keys_serialize_unchanged() throws Exception {
        assertEquals("{\"environment\":\"prod\",\"name\":\"checkout\"}",
                new ObjectMapper().writeValueAsString(new Node.KeyAttributes("prod", "checkout")));
    }
}
