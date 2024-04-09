/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.validator;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Add this annotation to validate that a given field as a valid AWS account Id.
 */
@Constraint(validatedBy = {AwsAccountIdConstraintValidator.class})
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface AwsAccountId {
    String message() default "The value provided for an AWS account Id must be a valid AWS account Id with 12 digits.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
