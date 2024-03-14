/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation can be added to any plugin configuration field that is supposed to support secret string value.
 */
@Documented
@Retention(RUNTIME)
@Target({ElementType.FIELD})
public @interface SupportSecretString {
}
