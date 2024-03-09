/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@Named
public class DefaultEventFactory implements EventFactory {
    private final Map<Class<?>, DefaultEventBuilderFactory> classToFactoryMap;

    @Inject
    DefaultEventFactory(final Collection<DefaultEventBuilderFactory> factories) {
        classToFactoryMap = factories.stream()
                .collect(Collectors.toMap(DefaultEventBuilderFactory::getEventClass, v -> v));
    }

    @Override
    public <T extends Event, B extends BaseEventBuilder<T>> B eventBuilder(final Class<B> eventBuilderClass) throws UnsupportedOperationException {
        if (!classToFactoryMap.containsKey(eventBuilderClass)) {
            throw new UnsupportedOperationException("Unsupported class");
        }

        return (B) classToFactoryMap.get(eventBuilderClass).createNew();
    }
}
