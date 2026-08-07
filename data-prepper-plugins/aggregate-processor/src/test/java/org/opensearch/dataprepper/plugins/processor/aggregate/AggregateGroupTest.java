/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AggregateGroupTest {

    private static final Duration TEST_GROUP_DURATION = Duration.ofSeconds(new Random().nextInt(10) + 10);

    @Test
    void resetGroup_after_getting_group_state_clears_group_state() {
        final AggregateGroup aggregateGroup = new AggregateGroup(null);

        final GroupState groupState = aggregateGroup.getGroupState();
        groupState.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        aggregateGroup.resetGroup();

        assertThat(groupState, equalTo(Collections.emptyMap()));
    }

    @Test
    void shouldConcludeGroup_returns_true_when_duration_is_over() throws NoSuchFieldException, IllegalAccessException {
        final AggregateGroup aggregateGroup = new AggregateGroup(null);
        reflectivelySetField(aggregateGroup, "groupStart", Instant.now().minusSeconds(TEST_GROUP_DURATION.getSeconds()));

        assertThat(aggregateGroup.shouldConcludeGroup(TEST_GROUP_DURATION), equalTo(true));

    }

    @Test
    void shouldConcludeGroup_returns_false_when_duration_is_not_over() throws NoSuchFieldException, IllegalAccessException {
        final AggregateGroup aggregateGroup = new AggregateGroup(null);
        reflectivelySetField(aggregateGroup, "groupStart", Instant.now().plusSeconds(TEST_GROUP_DURATION.getSeconds()));

        assertThat(aggregateGroup.shouldConcludeGroup(TEST_GROUP_DURATION), equalTo(false));
    }

    private void reflectivelySetField(final AggregateGroup aggregateGroup, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = AggregateGroup.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(aggregateGroup, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
