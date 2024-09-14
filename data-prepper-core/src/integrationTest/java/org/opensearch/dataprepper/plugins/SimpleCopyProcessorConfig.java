/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

public class SimpleCopyProcessorConfig {
    @EventKeyConfiguration(EventKeyFactory.EventAction.GET)
    private EventKey source;
    private EventKey target;

    public EventKey getSource() {
        return source;
    }

    public EventKey getTarget() {
        return target;
    }
}
