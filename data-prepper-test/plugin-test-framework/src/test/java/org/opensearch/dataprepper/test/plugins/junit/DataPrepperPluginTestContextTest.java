/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataPrepperPluginTestContextTest {
    private String pluginName;
    private Class<?> pluginType;

    @BeforeEach
    void setUp() {
        pluginName = UUID.randomUUID().toString();
        pluginType = String.class;
    }

    private DataPrepperPluginTestContext createObjectUnderTest() {
        return new DataPrepperPluginTestContext(pluginName, pluginType);
    }

    @Test
    void constructor_throws_with_null_pluginName() {
        pluginName = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_pluginType() {
        pluginType = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getters_return_values_from_constructor() {
        final DataPrepperPluginTestContext objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getPluginName(), equalTo(pluginName));
        assertThat(objectUnderTest.getPluginType(), equalTo(pluginType));
    }
}