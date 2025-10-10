/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.Map;
import java.util.function.Supplier;

class EventParameterResolver implements ParameterResolver {
    private static final Map<Class<?>, Supplier<?>> SUPPORTED_CLASSES = Map.of(
            EventFactory.class, TestEventFactory::getTestEventFactory,
            EventKeyFactory.class, TestEventKeyFactory::getTestEventFactory
    );

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        return SUPPORTED_CLASSES.containsKey(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        final Class<?> type = parameterContext.getParameter().getType();

        return SUPPORTED_CLASSES.get(type).get();
    }
}
