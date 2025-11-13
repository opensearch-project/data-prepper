/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

public class DefaultSinkFlushResult implements SinkFlushResult {
    private final List<Event> events;
    private final Throwable exception;
    private final int statusCode;

    public DefaultSinkFlushResult(final List<Event> events, final int statusCode, final Throwable exception) {
        this.events = events;
        this.exception = exception;
        this.statusCode = statusCode;
    }

    public List<Event> getEvents() {
        return events;
    }

    public Throwable getException() {
        return exception;
    }

    public int getStatusCode() {
        return statusCode;
    }
}

