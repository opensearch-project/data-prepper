/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
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
