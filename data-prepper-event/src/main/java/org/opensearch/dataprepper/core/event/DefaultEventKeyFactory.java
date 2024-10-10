/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.InternalOnlyEventKeyBridge;

class DefaultEventKeyFactory implements EventKeyFactory {
    @Override
    public EventKey createEventKey(final String key, final EventAction... forActions) {
        return InternalOnlyEventKeyBridge.createEventKey(key, forActions);
    }
}
