/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.event.DefaultLogEventBuilder;

public class DefaultEventFactory implements EventFactory {
    @Override
    public <T extends Event, B extends BaseEventBuilder<T>> B eventBuilder(Class<B> eventBuilderClass) {
        if (eventBuilderClass.equals(LogEventBuilder.class)) {
            return (B) new DefaultLogEventBuilder();
        }
        if (eventBuilderClass.equals(DefaultEventBuilder.class)) {
            return (B) new DefaultEventBuilder();
        }
        return null;
    }
}
