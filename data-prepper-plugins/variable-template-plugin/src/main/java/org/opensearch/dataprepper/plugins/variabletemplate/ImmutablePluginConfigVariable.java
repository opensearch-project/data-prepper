/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.opensearch.dataprepper.model.plugin.FailedToUpdatePluginConfigValueException;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

public class ImmutablePluginConfigVariable implements PluginConfigVariable {

    private final Object value;

    public ImmutablePluginConfigVariable(final Object value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setValue(final Object newValue) {
        throw new FailedToUpdatePluginConfigValueException("This variable is immutable and cannot be updated.");
    }

    @Override
    public void refresh() {
    }

    @Override
    public boolean isUpdatable() {
        return false;
    }
}
