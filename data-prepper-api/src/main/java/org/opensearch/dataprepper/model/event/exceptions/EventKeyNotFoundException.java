/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event.exceptions;

public class EventKeyNotFoundException extends RuntimeException {
    public EventKeyNotFoundException(final String message) {
        super(message);
    }
}
