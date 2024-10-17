/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.parser.DataFlowComponent;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class DataFlowComponentTest {

    private Object component;
    private Set<String> routes;

    @BeforeEach
    void setUp() {
        component = mock(Object.class);
        routes = Collections.singleton(UUID.randomUUID().toString());
    }

    private DataFlowComponent createObjectUnderTest() {
        return new DataFlowComponent(component, routes);
    }

    @Test
    void constructor_throws_with_null_component() {
        component = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_routes() {
        routes = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getComponent_returns_input_component() {
        assertThat(createObjectUnderTest().getComponent(), equalTo(component));
    }

    @Test
    void getRoutes_returns_input_routes() {
        assertThat(createObjectUnderTest().getRoutes(), equalTo(routes));
    }

    @Test
    void getRoutes_returns_input_routes_when_empty() {
        routes = Collections.emptySet();
        assertThat(createObjectUnderTest().getRoutes(), equalTo(routes));
    }
}