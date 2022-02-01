/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import java.util.HashMap;

public class AggregateActionTestUtils {
    public static class TestAggregateActionInput implements AggregateActionInput {
        private final GroupState groupState;

        public TestAggregateActionInput() {
            this.groupState = new AggregateActionTestUtils.TestGroupState();
        }

        @Override
        public GroupState getGroupState() {
            return groupState;
        }
    }

    public static class TestGroupState extends HashMap<Object, Object> implements GroupState {

    }
}
