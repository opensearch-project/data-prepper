/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

public class SimpleProcessorConfig {
    @EventKeyConfiguration(EventKeyFactory.EventAction.PUT)
    private EventKey key1;
    private String valuePrefix1;

    public EventKey getKey1() {
        return key1;
    }

    public String getValuePrefix1() {
        return valuePrefix1;
    }
}
