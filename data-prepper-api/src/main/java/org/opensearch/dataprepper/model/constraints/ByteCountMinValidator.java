/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.constraints;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.opensearch.dataprepper.model.types.ByteCount;

public class ByteCountMinValidator implements ConstraintValidator<ByteCountMin, ByteCount> {
    private ByteCount minByteCount;

    @Override
    public void initialize(final ByteCountMin constraint) {
        minByteCount = ByteCount.parse(constraint.value());
    }

    @Override
    public boolean isValid(final ByteCount byteCount, final ConstraintValidatorContext context) {
        if (byteCount == null) {
            return true;
        }

        if (byteCount.compareTo(minByteCount) < 0) {
            if (context != null) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(
                        String.format("The provided byte count %s is below minimum of %s", byteCount, minByteCount))
                        .addConstraintViolation();
            }

            return false;
        }

        return true;
    }
}
