/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.core.event.EventConfigurationContainer;
import org.opensearch.dataprepper.core.event.EventFactoryApplicationContextMarker;
import org.opensearch.dataprepper.core.event.TestEventConfigurationContainer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

class TestEventContext {
    private static AnnotationConfigApplicationContext APPLICATION_CONTEXT;

    private TestEventContext() {}

    static <T> T getFromContext(final Class<T> targetClass) {
        if(APPLICATION_CONTEXT == null) {
            APPLICATION_CONTEXT = new AnnotationConfigApplicationContext();
            APPLICATION_CONTEXT.registerBean(EventConfigurationContainer.class, () -> TestEventConfigurationContainer::testEventConfiguration);
            APPLICATION_CONTEXT.scan(EventFactoryApplicationContextMarker.class.getPackageName());
            APPLICATION_CONTEXT.refresh();
        }
        return APPLICATION_CONTEXT.getBean(targetClass);
    }
}
