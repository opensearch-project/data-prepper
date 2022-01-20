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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class GroupStateManagerTest {

    private GroupStateManager groupStateManager;
    private Map<Object, Object> identificationKeysHash;
    private Map<Object, Object> startingGroupState;

    @BeforeEach
    void setup() {
        identificationKeysHash = new HashMap<>();
        identificationKeysHash.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        startingGroupState = new HashMap<>();
        startingGroupState.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private GroupStateManager createObjectUnderTest() {
        return new GroupStateManager();
    }

    @Test
    void getGroupState_with_existing_group_state_returns_correct_group_state() {
        groupStateManager = createObjectUnderTest();
        groupStateManager.setGroupStateForIdentificationKeysHash(identificationKeysHash, startingGroupState);

        final Map<Object, Object> resultingGroupState = groupStateManager.getGroupState(identificationKeysHash);
        assertThat(resultingGroupState, equalTo(startingGroupState));
    }

    @Test
    void getGroupState_with_non_existing_group_state_creates_and_returns_new_group_state_and_adds_to_allGroupStates() {
        groupStateManager = createObjectUnderTest();

        final Map<Object, Object> emptyGroupState = groupStateManager.getGroupState(identificationKeysHash);
        assertThat(emptyGroupState, equalTo(Collections.emptyMap()));

        final Map<Object, Map<Object, Object>> allGroupStates = groupStateManager.getAllGroupStates();
        assertThat(allGroupStates, hasKey(identificationKeysHash));
    }
}
