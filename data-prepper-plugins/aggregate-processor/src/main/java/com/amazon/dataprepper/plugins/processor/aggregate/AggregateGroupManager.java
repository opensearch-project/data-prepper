/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.google.common.collect.Maps;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AggregateGroupManager {

    private final Map<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> allGroups = Maps.newConcurrentMap();
    private final Duration windowDuration;

    AggregateGroupManager(final int windowDuration) {
        this.windowDuration = Duration.ofSeconds(windowDuration);
    }

    AggregateGroup getAggregateGroup(final AggregateIdentificationKeysHasher.IdentificationHash identificationHash) {
        return allGroups.computeIfAbsent(identificationHash, (hash) -> new AggregateGroup());
    }

    List<Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>> getGroupsToConclude() {
        final List<Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>> groupsToConclude = new ArrayList<>();
        for (final Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> groupEntry : allGroups.entrySet()) {
            if (shouldConcludeGroup(groupEntry.getValue())) {
                groupsToConclude.add(groupEntry);
            }
        }
        return groupsToConclude;
    }

    private boolean shouldConcludeGroup(final AggregateGroup aggregateGroup) {
        return Duration.between(aggregateGroup.getGroupStart(), Instant.now()).compareTo(windowDuration) >= 0;
    }

    void closeGroup(final AggregateIdentificationKeysHasher.IdentificationHash hash, final AggregateGroup group) {
        allGroups.remove(hash, group);
        group.resetGroup();
    }

    void putGroupWithHash(final AggregateIdentificationKeysHasher.IdentificationHash hash, final AggregateGroup group) {
        allGroups.put(hash, group);
    }

    long getAllGroupsSize() {
        return allGroups.size();
    }
}
