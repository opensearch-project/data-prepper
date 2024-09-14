/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

/**
 * Until we remove {@link JacksonEvent} from data-prepper-api,
 * we will need this class to give us access to the package-protected
 * {@link JacksonEventKey}.
 */
public class InternalOnlyEventKeyBridge {
    public static EventKey createEventKey(final String key, final EventKeyFactory.EventAction... forAction) {
        return new JacksonEventKey(key, forAction);
    }
}
