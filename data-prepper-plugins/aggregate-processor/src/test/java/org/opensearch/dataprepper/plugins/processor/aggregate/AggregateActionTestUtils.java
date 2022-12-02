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
        private final Map<Object, Object> identificationKeyMap;

        public TestAggregateActionInput(Map<Object, Object> identificationKeyMap) {
            this.groupState = new AggregateActionTestUtils.TestGroupState();
            this.identificationKeyMap = identificationKeyMap;
        }

        @Override
        public GroupState getGroupState() {
            return groupState;
        }

        @Override
        public Map<Object, Object> getIdentificationKeysMap() {
            return identificationKeyMap;
        }
    }

    public static class TestGroupState extends HashMap<Object, Object> implements GroupState {

    }
}
