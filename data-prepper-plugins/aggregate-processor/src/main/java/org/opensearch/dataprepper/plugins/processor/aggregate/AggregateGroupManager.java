/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import com.google.common.collect.Maps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AggregateGroupManager {

    private final Map<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> allGroups = Maps.newConcurrentMap();
    private final Duration groupDuration;

    AggregateGroupManager(final Duration groupDuration) {
        this.groupDuration = groupDuration;
    }

    AggregateGroup getAggregateGroup(final AggregateIdentificationKeysHasher.IdentificationKeysMap identificationKeysMap) {
        return allGroups.computeIfAbsent(identificationKeysMap, (hash) -> new AggregateGroup(identificationKeysMap.getKeyMap()));
    }

    List<Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>> getGroupsToConclude(final boolean forceConclude) {
        final List<Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>> groupsToConclude = new ArrayList<>();
        for (final Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> groupEntry : allGroups.entrySet()) {
            if (groupEntry.getValue().shouldConcludeGroup(groupDuration) || forceConclude) {
                groupsToConclude.add(groupEntry);
            }
        }
        return groupsToConclude;
    }

    void closeGroup(final AggregateIdentificationKeysHasher.IdentificationKeysMap hashKeyMap, final AggregateGroup group) {
        allGroups.remove(hashKeyMap, group);
        group.resetGroup();
    }

    void putGroupWithHash(final AggregateIdentificationKeysHasher.IdentificationKeysMap hashKeyMap, final AggregateGroup group) {
        allGroups.put(hashKeyMap, group);
    }

    long getAllGroupsSize() {
        return allGroups.size();
    }

    Duration getGroupDuration() {
        return this.groupDuration;
    }
}
