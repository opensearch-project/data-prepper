/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

public class HecEventValidationException extends RuntimeException {

    private final int eventNumber;

    public HecEventValidationException(final int eventNumber) {
        super("Event field is required at event number " + eventNumber);
        this.eventNumber = eventNumber;
    }

    public int getEventNumber() {
        return eventNumber;
    }
}
