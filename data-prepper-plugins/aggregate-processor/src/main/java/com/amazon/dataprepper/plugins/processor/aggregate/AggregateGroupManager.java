/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.google.common.collect.Maps;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AggregateGroupManager {

    private final Map<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> allGroups = Maps.newConcurrentMap();
    private final int windowDuration;

    AggregateGroupManager(final int windowDuration) {
        this.windowDuration = windowDuration;
    }

    AggregateGroup getAggregateGroup(final AggregateIdentificationKeysHasher.IdentificationHash identificationHash) {
        final AggregateGroup aggregateGroup = allGroups.get(identificationHash);

        if (aggregateGroup != null) {
            return aggregateGroup;
        }

        final AggregateGroup newAggregateGroup = new AggregateGroup();
        allGroups.put(identificationHash, newAggregateGroup);
        return newAggregateGroup;
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
        return (Instant.now().toEpochMilli() - aggregateGroup.getGroupStart().toEpochMilli()) >= windowDuration * 1000L;
    }

    void removeGroupWithHash(final AggregateIdentificationKeysHasher.IdentificationHash hash, final AggregateGroup group) {
        allGroups.remove(hash, group);
    }

    void putGroupWithHash(final AggregateIdentificationKeysHasher.IdentificationHash hash, final AggregateGroup group) {
        allGroups.put(hash, group);
    }
}
