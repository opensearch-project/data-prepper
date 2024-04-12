/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.validator;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

/**
 * Validates than a value is a valid AWS account Id. This is intended for internal use
 * only, but must be public to work with bean validation.
 */
public class AwsAccountIdConstraintValidator implements ConstraintValidator<AwsAccountId, String> {
    @Override
    public boolean isValid(final String string, final ConstraintValidatorContext constraintValidatorContext) {
        if(string == null)
            return true;

        if(string.length() != 12)
            return false;

        for(int i = 0; i < string.length(); i++) {
            if(!Character.isDigit(string.charAt(i)))
                return false;
        }

        return true;
    }
}
