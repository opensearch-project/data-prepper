/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.log;

import com.amazon.dataprepper.model.event.EventType;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.joda.time.LocalDateTime;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Log}. This class extends the {@link JacksonEvent}.
 */
public class JacksonLog extends JacksonEvent implements Log {

    protected JacksonLog(final Builder builder) {
        super(builder);
        this.put("event_timestamp", LocalDateTime.now());

        checkArgument(this.getMetadata().getEventType().equals("LOG"), "eventType must be of type Log");
    }

    /**
     * Constructs an empty builder.
     * @return a builder
     * @since 1.2
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating {@link JacksonLog}.
     * @since 1.2
     */
    public static class Builder extends JacksonEvent.Builder<Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Returns a newly created {@link JacksonLog}.
         * @return a log
         * @since 1.2
         */
        @Override
        public JacksonLog build() {
            this.withEventType(EventType.LOG.toString());
            return new JacksonLog(this);
        }
    }
}
