/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
