/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;

public class AggregateGroupManagerTest {

    private AggregateGroupManager aggregateGroupManager;

    private AggregateIdentificationKeysHasher.IdentificationHash identificationHash;

    @BeforeEach
    void setup() {
        final Map<Object, Object> identificationKeysHash = new HashMap<>();
        identificationKeysHash.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        identificationHash = new AggregateIdentificationKeysHasher.IdentificationHash(identificationKeysHash);
    }

    private AggregateGroupManager createObjectUnderTest() {
        return new AggregateGroupManager();
    }

    @Test
    void getGroupState_with_non_existing_group_state_creates_and_returns_new_group_state_and_adds_to_allGroupStates() {
        aggregateGroupManager = createObjectUnderTest();

        final AggregateGroup emptyAggregateGroup = aggregateGroupManager.getAggregateGroup(identificationHash);
        assertThat(emptyAggregateGroup, notNullValue());
        assertThat(emptyAggregateGroup.getGroupState(), equalTo(Collections.emptyMap()));

        final AggregateGroup secondAggregateGroup = aggregateGroupManager.getAggregateGroup(identificationHash);
        assertThat(secondAggregateGroup, notNullValue());
        assertThat(secondAggregateGroup, is(sameInstance(emptyAggregateGroup)));
    }

    @Test
    void getGroupState_returns_a_mutable_GroupState_Map() {
        aggregateGroupManager = createObjectUnderTest();

        final AggregateGroup firstAggregateGroup = aggregateGroupManager.getAggregateGroup(identificationHash);
        firstAggregateGroup.getGroupState().put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        final AggregateGroup secondAggregateGroup = aggregateGroupManager.getAggregateGroup(identificationHash);
        assertThat(secondAggregateGroup, equalTo(firstAggregateGroup));

    }
}
