/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;

import java.util.Optional;

/**
 * Model class to be returned in {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateAction}. Contains both the Event to be processed and an option
 * to close the current window for {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateProcessor} immediately after an event is handled.
 * @since 1.3
 */
public class AggregateActionResponse {
    private Optional<Event> event;
    private boolean closeWindowNow;

    public AggregateActionResponse(Optional<Event> event, boolean closeWindowNow) {
        this.event = event;
        this.closeWindowNow = closeWindowNow;
    }

    public Optional<Event> getEvent() {
        return event;
    }

    public boolean isCloseWindowNow() {
        return closeWindowNow;
    }
}
