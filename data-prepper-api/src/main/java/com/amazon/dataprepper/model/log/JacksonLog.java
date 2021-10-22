package com.amazon.dataprepper.model.log;

import com.amazon.dataprepper.model.event.EventType;
import com.amazon.dataprepper.model.event.JacksonEvent;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Log}. This class extends the {@link JacksonEvent}.
 */
public class JacksonLog extends JacksonEvent implements Log {

    protected JacksonLog(final Builder builder) {
        super(builder);

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
        public JacksonLog build() {
            this.withEventType(EventType.LOG.toString());
            return new JacksonLog(this);
        }
    }
}
