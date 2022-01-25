/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import java.util.Map;

import com.google.common.collect.Maps;

class AggregateGroupManager {

    private final Map<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> allGroupStates = Maps.newConcurrentMap();

    AggregateGroupManager() {

    }

    AggregateGroup getAggregateGroup(final AggregateIdentificationKeysHasher.IdentificationHash identificationHash) {
        final AggregateGroup aggregateGroup = allGroupStates.get(identificationHash);

        if (aggregateGroup != null) {
            return aggregateGroup;
        }

        final AggregateGroup newAggregateGroup = new AggregateGroup();
        allGroupStates.put(identificationHash, newAggregateGroup);
        return newAggregateGroup;
    }
}
