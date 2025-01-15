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
 * Marks a Data Prepper plugin as experimental.
 * <p>
 * Experimental plugins do not have the same compatibility guarantees as other plugins and may be unstable.
 * They may have breaking changes between minor versions and may even be removed.
 * <p>
 * Data Prepper administrators must enable experimental plugins in order to use them.
 * Otherwise, they are not available to use with pipelines.
 *
 * @since 2.11
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Experimental {
}
