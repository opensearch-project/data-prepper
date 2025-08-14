/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation for configuring plugin tests for a given plugin.
 *
 * @since 2.13
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DataPrepperPluginTest {
    /**
     * Provides the name of the plugin.
     *
     * @since 2.13
     * @return the plugin name
     */
    String pluginName();

    /**
     * Configures the type of the plugin. This should be the same type
     * that is used with Data Prepper, such as {@link org.opensearch.dataprepper.model.processor.Processor}.
     *
     * @since 2.13
     * @return The class type of the plugin.
     */
    Class<?> pluginType();
}
