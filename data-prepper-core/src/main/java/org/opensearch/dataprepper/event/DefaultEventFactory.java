/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;

import java.util.Map;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.inject.Named;
import javax.inject.Inject;

@Named
public class DefaultEventFactory implements EventFactory {
    Map<Class<?>, DefaultEventBuilderFactory> classToFactoryMap;

    @Inject
    public DefaultEventFactory(Collection< DefaultEventBuilderFactory> factories) {
        classToFactoryMap = factories.stream()
                  .collect(Collectors.toMap(DefaultEventBuilderFactory::getEventClass, v -> v));   
    }
    
    @Override
    public <T extends Event, B extends BaseEventBuilder<T>> B eventBuilder(Class<B> eventBuilderClass) throws UnsupportedOperationException {
        if (!classToFactoryMap.containsKey(eventBuilderClass)) {
            throw new UnsupportedOperationException("Unsupported class");
        }
        
        return (B) classToFactoryMap.get(eventBuilderClass).createNew();
    }
}
