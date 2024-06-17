/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Injects types from the Application Context and makes them available
 * for plugin constructors.
 * <p>
 * If you are providing a direct pass-through from the Application Context
 * to a plugin constructor, add it here. It is the simplest place for it.
 * Otherwise, you can add it to the {@link DefaultPluginFactory} directly.
 */
@Named
class ApplicationContextToTypedSuppliers {
    private final Map<Class<?>, Supplier<Object>> typedSuppliers;

    @Inject
    ApplicationContextToTypedSuppliers(
            final EventFactory eventFactory,
            final EventKeyFactory eventKeyFactory,
            final AcknowledgementSetManager acknowledgementSetManager,
            @Autowired(required = false) final CircuitBreaker circuitBreaker
    ) {
        Objects.requireNonNull(eventFactory);
        Objects.requireNonNull(acknowledgementSetManager);

        typedSuppliers = Map.of(
                EventFactory.class, () -> eventFactory,
                EventKeyFactory.class, () -> eventKeyFactory,
                AcknowledgementSetManager.class, () -> acknowledgementSetManager,
                CircuitBreaker.class, () -> circuitBreaker
        );
    }

    /**
     * Gets a map of {@link Class} to a {@link Supplier} for each class. The
     * {@link Supplier} supplies an instance of that type. The supplied value
     * may be <b>null</b> for some types.
     *
     * @return Map of types to suppliers of concrete implementations.
     */
    Map<Class<?>, Supplier<Object>> getArgumentsSuppliers() {
        return typedSuppliers;
    }
}
