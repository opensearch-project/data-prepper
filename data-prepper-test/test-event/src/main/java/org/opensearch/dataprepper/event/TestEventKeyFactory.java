/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

public class TestEventKeyFactory implements EventKeyFactory {
    private static EventKeyFactory DEFAULT_EVENT_KEY_FACTORY;
    private final EventKeyFactory innerEventKeyFactory;

    TestEventKeyFactory(final EventKeyFactory innerEventKeyFactory) {
        this.innerEventKeyFactory = innerEventKeyFactory;
    }

    public static EventKeyFactory getTestEventFactory() {
        if(DEFAULT_EVENT_KEY_FACTORY == null) {
            DEFAULT_EVENT_KEY_FACTORY = TestEventContext.getFromContext(EventKeyFactory.class);
        }
        return new TestEventKeyFactory(DEFAULT_EVENT_KEY_FACTORY);
    }

    @Override
    public EventKey createEventKey(final String key, final EventAction... forActions) {
        return innerEventKeyFactory.createEventKey(key, forActions);
    }
}
