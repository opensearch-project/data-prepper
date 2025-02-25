/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.exceptions;

public class KinesisStreamNotFoundException extends RuntimeException {
    public KinesisStreamNotFoundException(final String errorMessage) {
        super(errorMessage);
    }
}
