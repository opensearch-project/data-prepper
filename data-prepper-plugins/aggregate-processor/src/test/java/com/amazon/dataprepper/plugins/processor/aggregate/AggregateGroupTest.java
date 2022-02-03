/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AggregateGroupTest {

    @Test
    void resetGroup_after_getting_group_state_clears_group_state() {
        final AggregateGroup aggregateGroup = new AggregateGroup();

        final GroupState groupState = aggregateGroup.getGroupState();
        groupState.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        aggregateGroup.resetGroup();

        assertThat(groupState, equalTo(Collections.emptyMap()));
    }
}
