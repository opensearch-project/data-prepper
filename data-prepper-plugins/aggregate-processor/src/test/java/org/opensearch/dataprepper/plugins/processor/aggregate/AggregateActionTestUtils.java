/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import java.util.Map;
import java.util.HashMap;

public class AggregateActionTestUtils {
    public static class TestAggregateActionInput implements AggregateActionInput {
        private final GroupState groupState;
        private final Map<Object, Object> identificationKeys;

        public TestAggregateActionInput(Map<Object, Object> identificationKeys) {
            this.groupState = new AggregateActionTestUtils.TestGroupState();
            this.identificationKeys = identificationKeys;
        }

        @Override
        public GroupState getGroupState() {
            return groupState;
        }

        @Override
        public Map<Object, Object> getIdentificationKeys() {
            return identificationKeys;
        }
    }

    public static class TestGroupState extends HashMap<Object, Object> implements GroupState {

    }
}
