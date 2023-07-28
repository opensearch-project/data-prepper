/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.dlq;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.util.Objects;

public class FailedDlqData {

    private final int status;

    private final String message;

    @JsonIgnore
    private final EventHandle eventHandle;

    private FailedDlqData(final int status,
                          final String message,
                          final EventHandle eventHandle) {
        this.status = status;
        Objects.requireNonNull(message);
        this.message = message;
        this.eventHandle = eventHandle;
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
    public EventHandle getEventHandle() {
        return eventHandle;
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private EventHandle eventHandle;

        private int status = 0;

        private String message;

        public FailedDlqData build() {
            return new FailedDlqData(status, message, eventHandle);
        }
    }
}
