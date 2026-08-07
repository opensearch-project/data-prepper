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
 * Annotates a Data Prepper extension plugin which includes a configuration model class.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DataPrepperExtensionPlugin {
    /**
     * @return extension plugin configuration class.
     */
    Class<?> modelType();

    /**
     * @return valid JSON path string starts with "/" pointing towards the configuration block.
     */
    String rootKeyJsonPath();

    boolean allowInPipelineConfigurations() default false;
}
