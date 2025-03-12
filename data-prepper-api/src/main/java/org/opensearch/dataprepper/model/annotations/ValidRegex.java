/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.annotations;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import org.opensearch.dataprepper.model.validation.RegexValueValidator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Constraint(validatedBy = RegexValueValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidRegex {
    String message() default "Invalid regular expression pattern";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
