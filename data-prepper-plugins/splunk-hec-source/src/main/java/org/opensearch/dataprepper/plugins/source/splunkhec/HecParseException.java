/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import java.io.IOException;

public class HecParseException extends IOException {

    private final int eventNumber;

    public HecParseException(final int eventNumber, final Throwable cause) {
        super("Failed to parse event at index " + eventNumber, cause);
        this.eventNumber = eventNumber;
    }

    public int getEventNumber() {
        return eventNumber;
    }
}
