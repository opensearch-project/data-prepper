/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import java.util.Map;

import com.google.common.collect.Maps;

class GroupStateManager {

    private final Map<AggregateIdentificationKeysHasher.IdentificationHash, GroupState> allGroupStates = Maps.newConcurrentMap();

    GroupStateManager() {

    }

    GroupState getGroupState(final AggregateIdentificationKeysHasher.IdentificationHash identificationHash) {
        final GroupState groupState = allGroupStates.get(identificationHash);

        if (groupState != null) {
            return groupState;
        }

        final GroupState newGroupState = new GroupState();
        allGroupStates.put(identificationHash, newGroupState);
        return newGroupState;
    }
}
