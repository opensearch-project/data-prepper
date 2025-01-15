/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.utils;

import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

/**
 * Mock implementation of PluginConfigVariable interface used only for Unit Testing.
 */
public class MockPluginConfigVariableImpl implements PluginConfigVariable {

    private Object defaultValue;
    
    public MockPluginConfigVariableImpl(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public void setValue(Object someValue) {
        this.defaultValue = someValue;
    }
}
