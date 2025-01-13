/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.model.plugin;

/**
 * Interface for a Extension Plugin configuration variable.
 * It gives access to the details of a defined extension variable.
 *
 * @since 1.2
 */
public interface PluginConfigVariable {

    /**
     * Returns the value of this variable.
     */
    Object getValue();

    /**
     * If this variable is updatable, this method helps to set a new value for this variable
     */
    void setValue(Object someValue);
}
