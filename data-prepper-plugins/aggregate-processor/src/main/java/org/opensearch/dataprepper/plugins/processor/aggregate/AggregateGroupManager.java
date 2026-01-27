/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import com.google.common.collect.Maps;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.hasher.IdentificationKeysHasher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AggregateGroupManager {

    private final Map<IdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> allGroups = Maps.newConcurrentMap();
    private final Duration groupDuration;
    private final boolean acknowledgeOnConclude;

    AggregateGroupManager(final Duration groupDuration, final boolean acknowledgeOnConclude) {
        this.groupDuration = groupDuration;
        this.acknowledgeOnConclude = acknowledgeOnConclude;
    }

    AggregateGroup getAggregateGroup(final IdentificationKeysHasher.IdentificationKeysMap identificationKeysMap) {
        return allGroups.computeIfAbsent(identificationKeysMap, (hash) -> new AggregateGroup(identificationKeysMap.getKeyMap()));
    }


    List<Map.Entry<IdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>> getGroupsToConclude(final boolean forceConclude) {
        final List<Map.Entry<IdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>> groupsToConclude = new ArrayList<>();
        for (final Map.Entry<IdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> groupEntry : allGroups.entrySet()) {
            if (groupEntry.getValue().shouldConcludeGroup(groupDuration) || forceConclude) {
                groupsToConclude.add(groupEntry);
            }
        }
        return groupsToConclude;
    }

    void closeGroup(final IdentificationKeysHasher.IdentificationKeysMap hashKeyMap, final AggregateGroup group) {
        allGroups.remove(hashKeyMap, group);

        if (acknowledgeOnConclude) {
            EventHandle handle = group.getEventHandle();
            if (handle != null) {
                handle.release(true);
            }
        }
        group.resetGroup();
    }

    void putGroupWithHash(final IdentificationKeysHasher.IdentificationKeysMap hashKeyMap, final AggregateGroup group) {
        allGroups.put(hashKeyMap, group);
    }

    long getAllGroupsSize() {
        return allGroups.size();
    }

    Duration getGroupDuration() {
        return this.groupDuration;
    }
}
