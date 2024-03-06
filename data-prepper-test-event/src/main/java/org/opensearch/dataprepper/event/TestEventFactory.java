/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.core.event.EventFactoryApplicationContextMarker;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * An implementation of {@link EventFactory} that is useful for integration and unit tests
 * in other projects.
 */
public class TestEventFactory implements EventFactory {
    private static AnnotationConfigApplicationContext APPLICATION_CONTEXT;
    private static EventFactory DEFAULT_EVENT_FACTORY;
    private final EventFactory innerEventFactory;

    TestEventFactory(final EventFactory innerEventFactory) {
        this.innerEventFactory = innerEventFactory;
    }

    public static EventFactory getTestEventFactory() {
        if(APPLICATION_CONTEXT == null) {
            APPLICATION_CONTEXT = new AnnotationConfigApplicationContext();
            APPLICATION_CONTEXT.scan(EventFactoryApplicationContextMarker.class.getPackageName());
            APPLICATION_CONTEXT.refresh();
            DEFAULT_EVENT_FACTORY = APPLICATION_CONTEXT.getBean(EventFactory.class);
        }
        return new TestEventFactory(DEFAULT_EVENT_FACTORY);
    }

    @Override
    public <T extends Event, B extends BaseEventBuilder<T>> B eventBuilder(final Class<B> eventBuilderClass) throws UnsupportedOperationException {
        return innerEventFactory.eventBuilder(eventBuilderClass);
    }
}
