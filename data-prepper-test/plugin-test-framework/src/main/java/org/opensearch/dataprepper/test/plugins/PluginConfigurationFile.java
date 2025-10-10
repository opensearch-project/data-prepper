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
 * Specifies a configuration file for loading a plugin. Use this on a parameter of your
 * plugin type to define how to load the plugin from a configuration file.
 *
 * @since 2.13
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface PluginConfigurationFile {
    /**
     * The name of the configuration file to load from. This must be
     * in the same directory path as your test code package.
     * <p>
     * For example, <code>my_configuration.yaml</code>.
     *
     * @return The configuration file
     *
     * @since 2.13
     */
    String value();
}
