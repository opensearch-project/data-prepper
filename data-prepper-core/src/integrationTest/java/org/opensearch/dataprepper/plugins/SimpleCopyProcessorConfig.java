/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
