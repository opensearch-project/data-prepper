/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

class AggregateGroup implements AggregateActionInput {
    private final GroupState groupState;

    AggregateGroup() {
        this.groupState = new DefaultGroupState();
    }

    public GroupState getGroupState() {
        return groupState;
    }
}
