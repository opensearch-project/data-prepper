/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.buffer;

/**
 * Thrown to indicate the size of the data to be written into the {@link Buffer} is
 * beyond Buffer's maximum capacity.
 * <p>
 */
public class SizeOverflowException extends Exception {
    public SizeOverflowException(final String message) {
        super(message);
    }
}
