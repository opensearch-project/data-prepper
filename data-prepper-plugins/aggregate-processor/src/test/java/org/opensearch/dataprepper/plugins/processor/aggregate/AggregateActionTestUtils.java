/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.AggregateEventHandle;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.time.Duration;
import java.util.function.Function;

public class AggregateActionTestUtils {
    public static class TestAggregateActionInput implements AggregateActionInput {
        private final GroupState groupState;
        private final Map<Object, Object> identificationKeys;
        private Function<Duration, Boolean> customShouldConclude;
        private EventHandle eventHandle;

        public TestAggregateActionInput(Map<Object, Object> identificationKeys) {
            this.groupState = new AggregateActionTestUtils.TestGroupState();
            this.identificationKeys = identificationKeys;
            this.eventHandle = new AggregateEventHandle(Instant.now());
        }

        @Override
        public void setCustomShouldConclude(Function<Duration, Boolean> shouldConclude) {
            customShouldConclude = shouldConclude;
        }

        @Override
        public GroupState getGroupState() {
            return groupState;
        }

        @Override
        public EventHandle getEventHandle() {
            return eventHandle;
        }

        @Override
        public Map<Object, Object> getIdentificationKeys() {
            return identificationKeys;
        }

        public boolean shouldConcludeGroup(final Duration groupDuration) {
            if (customShouldConclude != null) {
                return customShouldConclude.apply(groupDuration);
            }
            return true;
        }
    }

    public static class TestGroupState extends HashMap<Object, Object> implements GroupState {

    }
}
