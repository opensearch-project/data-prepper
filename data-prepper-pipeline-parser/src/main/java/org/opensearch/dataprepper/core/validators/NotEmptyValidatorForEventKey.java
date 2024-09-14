/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.validators;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.opensearch.dataprepper.model.event.EventKey;

import jakarta.validation.constraints.NotEmpty;

public class NotEmptyValidatorForEventKey implements ConstraintValidator<NotEmpty, EventKey> {
    @Override
    public boolean isValid(final EventKey eventKey, final ConstraintValidatorContext constraintValidatorContext) {
        if(eventKey == null) {
            return false;
        }
        return !eventKey.getKey().isEmpty();
    }
}
