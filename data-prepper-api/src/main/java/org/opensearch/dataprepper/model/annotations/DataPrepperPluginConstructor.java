/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Add to a plugin class to indicate which constructor should be used by Data Prepper.
 * <p>
 * The current behavior for choosing a constructor is:
 * <ol>
 *     <li>Choose the constructor annotated with {@link DataPrepperPluginConstructor}</li>
 *     <li>Choose a constructor which takes in a single parameter matching
 *     the {@link DataPrepperPlugin#pluginConfigurationType()} for the plugin</li>
 *     <li>Use the default (ie. empty) constructor</li>
 * </ol>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.CONSTRUCTOR})
public @interface DataPrepperPluginConstructor {
}
