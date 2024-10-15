/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a part of the pipeline through which data flows. This includes
 * {@link org.opensearch.dataprepper.model.sink.Sink} and {@link org.opensearch.dataprepper.model.processor.Processor}.
 *
 * @param <T> The type of component.
 */
public class DataFlowComponent<T> {
    private final T component;
    private final Set<String> routes;

    DataFlowComponent(final T component, final Collection<String> routes) {
        this.component = Objects.requireNonNull(component);
        this.routes = new HashSet<>(Objects.requireNonNull(routes));
    }

    public T getComponent() {
        return component;
    }

    public Set<String> getRoutes() {
        return routes;
    }
}
