/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation can be added to classes or methods to exclude them from the jacoco test coverage report.
 * This can be used on simple Enums, basic model classes, and other generated code such as equals and toString methods
 * that are not considered covered in testing. There will be some cases where we add this annotation with plans to
 * add full test coverage
 */
@Documented
@Retention(RUNTIME)
@Target({TYPE, METHOD, CONSTRUCTOR})
public @interface SkipTestCoverageGenerated {
}
