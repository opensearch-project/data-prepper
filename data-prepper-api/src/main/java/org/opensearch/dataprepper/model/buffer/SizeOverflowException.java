/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.buffer;

/**
 * Thrown to indicate the size of the data to be written into the {@link Buffer} is
 * beyond Buffer's maximum capacity.
 */
public class SizeOverflowException extends Exception {
    public SizeOverflowException(final String message) {
        super(message);
    }
}
