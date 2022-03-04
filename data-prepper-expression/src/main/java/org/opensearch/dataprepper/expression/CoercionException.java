/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

public class CoercionException extends Exception {
    public CoercionException(final String message) {
        super(message);
    }
}
