/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Maps;

class GroupStateManager {

    private Map<Object, Map<Object, Object>> allGroupStates = Maps.newConcurrentMap();

    GroupStateManager() {

    }

    Map<Object, Object> getGroupState(final Map<Object, Object> identificationKeysHash) {
        final Optional<Map<Object, Object>> groupState = Optional.ofNullable(allGroupStates.get(identificationKeysHash));

        if (groupState.isPresent()) {
            return groupState.get();
        }

        final Map<Object, Object> newGroupState = new HashMap<>();
        allGroupStates.put(identificationKeysHash, newGroupState);
        return newGroupState;
    }

    void setGroupStateForIdentificationKeysHash(final Map<Object, Object> identificationKeysHash, final Map<Object, Object> groupState) {
        allGroupStates.put(identificationKeysHash, groupState);
    }

    Map<Object, Map<Object, Object>> getAllGroupStates() {
        return this.allGroupStates;
    }
}
