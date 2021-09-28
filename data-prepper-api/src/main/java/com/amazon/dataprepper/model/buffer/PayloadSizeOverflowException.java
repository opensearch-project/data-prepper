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

public class PayloadSizeOverflowException extends Exception {
    public PayloadSizeOverflowException(final Throwable cause) {
        super(cause);
    }

    public PayloadSizeOverflowException(final String message) {
        super(message);
    }

    public PayloadSizeOverflowException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
