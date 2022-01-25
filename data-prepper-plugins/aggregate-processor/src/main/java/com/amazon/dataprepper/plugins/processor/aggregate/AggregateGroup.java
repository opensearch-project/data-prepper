/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import java.util.HashMap;
import java.util.Map;

public class AggregateGroup {
    private final Map<Object, Object> groupState = new HashMap<>();

    AggregateGroup() {

    }

    public Map<Object, Object> getGroupState() {
        return groupState;
    }
}
