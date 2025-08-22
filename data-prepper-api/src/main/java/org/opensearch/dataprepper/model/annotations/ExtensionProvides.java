/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify that a class provides an extension and its dependencies.
 * This annotation can be used to declare what extension points a class provides
 * and what other extension points it depends on.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ExtensionProvides {
    /**
     * The list of classes that this extension provides
     * @return Array of Class objects representing the classes this extension provides
     */
    Class<?>[] providedClasses() default {};
}
