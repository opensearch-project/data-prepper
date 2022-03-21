/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregateGroupManagerTest {

    private AggregateGroupManager aggregateGroupManager;

    private AggregateIdentificationKeysHasher.IdentificationHash identificationHash;

    private static final Duration TEST_GROUP_DURATION = Duration.ofSeconds(new Random().nextInt(10) + 10);

    @BeforeEach
    void setup() {
        final Map<Object, Object> identificationKeysHash = new HashMap<>();
        identificationKeysHash.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        identificationHash = new AggregateIdentificationKeysHasher.IdentificationHash(identificationKeysHash);
    }

    private AggregateGroupManager createObjectUnderTest() {
        return new AggregateGroupManager(TEST_GROUP_DURATION);
    }

    @Test
    void getGroup_with_non_existing_group_state_creates_and_returns_new_group_and_adds_to_allGroups() {
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
        final GroupState groupState = firstAggregateGroup.getGroupState();
        groupState.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        final AggregateGroup secondAggregateGroup = aggregateGroupManager.getAggregateGroup(identificationHash);
        assertThat(secondAggregateGroup, equalTo(firstAggregateGroup));
        assertThat(secondAggregateGroup.getGroupState(), equalTo(groupState));

    }

    @Test
    void putGroupWithHash_overwrites_old_group() {
        aggregateGroupManager = createObjectUnderTest();

        final AggregateGroup expectedOldGroup = mock(AggregateGroup.class);
        aggregateGroupManager.putGroupWithHash(identificationHash, expectedOldGroup);
        final AggregateGroup oldGroup = aggregateGroupManager.getAggregateGroup(identificationHash);
        assertThat(oldGroup, equalTo(expectedOldGroup));

        final AggregateGroup expectedNewGroup = mock(AggregateGroup.class);
        aggregateGroupManager.putGroupWithHash(identificationHash, expectedNewGroup);
        final AggregateGroup newGroup = aggregateGroupManager.getAggregateGroup(identificationHash);
        assertThat(newGroup, equalTo(expectedNewGroup));
    }

    @Test
    void getGroupsToConclude_returns_correct_group() {
        aggregateGroupManager = createObjectUnderTest();

        final AggregateGroup groupToConclude = mock(AggregateGroup.class);
        when(groupToConclude.shouldConcludeGroup(TEST_GROUP_DURATION)).thenReturn(true);
        final AggregateIdentificationKeysHasher.IdentificationHash hashForGroupToConclude = mock(AggregateIdentificationKeysHasher.IdentificationHash.class);

        final AggregateGroup groupToNotConclude = mock(AggregateGroup.class);
        when(groupToNotConclude.shouldConcludeGroup(TEST_GROUP_DURATION)).thenReturn(false);
        final AggregateIdentificationKeysHasher.IdentificationHash hashForGroupToNotConclude = mock(AggregateIdentificationKeysHasher.IdentificationHash.class);

        aggregateGroupManager.putGroupWithHash(hashForGroupToConclude, groupToConclude);
        aggregateGroupManager.putGroupWithHash(hashForGroupToNotConclude, groupToNotConclude);

        final List<Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>> groupsToConclude = aggregateGroupManager.getGroupsToConclude();

        assertThat(groupsToConclude.size(), equalTo(1));
        assertThat(groupsToConclude.get(0), notNullValue());
        assertThat(groupsToConclude.get(0).getKey(), equalTo(hashForGroupToConclude));
        assertThat(groupsToConclude.get(0).getValue(), equalTo(groupToConclude));
    }
}
