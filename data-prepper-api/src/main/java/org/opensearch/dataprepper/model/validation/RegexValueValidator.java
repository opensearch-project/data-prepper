/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.opensearch.dataprepper.model.annotations.ValidRegex;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexValueValidator implements ConstraintValidator<ValidRegex, String> {
    @Override
    public boolean isValid(final String pattern, final ConstraintValidatorContext constraintValidatorContext) {
        if (pattern != null && !pattern.isEmpty()) {
            try {
                Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                return false;
            }
        }

        return true;
    }
}
