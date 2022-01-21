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

public class GroupStateManagerTest {

    private GroupStateManager groupStateManager;

    private AggregateIdentificationKeysHasher.IdentificationHash identificationHash;

    @BeforeEach
    void setup() {
        final Map<Object, Object> identificationKeysHash = new HashMap<>();
        identificationKeysHash.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        identificationHash = new AggregateIdentificationKeysHasher.IdentificationHash(identificationKeysHash);
    }

    private GroupStateManager createObjectUnderTest() {
        return new GroupStateManager();
    }

    @Test
    void getGroupState_with_non_existing_group_state_creates_and_returns_new_group_state_and_adds_to_allGroupStates() {
        groupStateManager = createObjectUnderTest();

        final GroupState emptyGroupState = groupStateManager.getGroupState(identificationHash);
        assertThat(emptyGroupState, notNullValue());
        assertThat(emptyGroupState.getGroupState(), equalTo(Collections.emptyMap()));

        final GroupState secondGroupState = groupStateManager.getGroupState(identificationHash);
        assertThat(secondGroupState, notNullValue());
        assertThat(secondGroupState, is(sameInstance(emptyGroupState)));
    }

    @Test
    void getGroupState_returns_a_mutable_GroupState_Map() {
        groupStateManager = createObjectUnderTest();

        final GroupState firstGroupState = groupStateManager.getGroupState(identificationHash);
        firstGroupState.getGroupState().put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        final GroupState secondGroupState = groupStateManager.getGroupState(identificationHash);
        assertThat(secondGroupState, equalTo(firstGroupState));

    }
}
