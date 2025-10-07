/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import java.util.HashMap;

public class DefaultEventFailureMetadata implements EventFailureMetadata {
    public static final String FAILURE_METADATA = "_failure_metadata";

    Event event;

    public DefaultEventFailureMetadata(Event event) {
        event.put(FAILURE_METADATA, new HashMap<>());
        this.event = event;
    }

    public EventFailureMetadata with(String key, Object value) {
        event.put(FAILURE_METADATA+"/"+key, value);
        return this;
    }

}
