/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.trace;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The default implementation of {@link SpanEvent}, timestamped annotation of associated attributes for a span.
 *
 * @since 1.2
 */
public class DefaultSpanEvent implements SpanEvent {

    private String name;
    private String time;
    private Map<String, Object> attributes;
    private Integer droppedAttributesCount;

    // required for serialization
    DefaultSpanEvent() {}

    private DefaultSpanEvent(final Builder builder) {
        checkNotNull(builder.name, "name cannot be null");
        checkArgument(!builder.name.isEmpty(), "name cannot be an empty string");
        this.name = builder.name;

        checkNotNull(builder.time, "time cannot be null");
        checkArgument(!builder.time.isEmpty(), "time cannot be an empty string");
        this.time = builder.time;

        this.attributes = builder.attributes == null ? new HashMap<>() : builder.attributes;
        this.droppedAttributesCount = builder.droppedAttributesCount == null ? 0 : builder.droppedAttributesCount;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTime() {
        return time;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public Integer getDroppedAttributesCount() {
        return droppedAttributesCount;
    }

    public boolean equals(final Object other) {

        if (other instanceof DefaultSpanEvent) {
            final DefaultSpanEvent o = (DefaultSpanEvent) other;
            return (name.equals(o.name) && time.equals(o.time) && attributes.equals(o.attributes)
                    && droppedAttributesCount.equals(o.droppedAttributesCount));
        }
        return false;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating {@link DefaultSpanEvent}
     * @since 1.2
     */
    public static class Builder {
        private String name;
        private String time;
        private Map<String, Object> attributes;
        private Integer droppedAttributesCount;

        /**
         * Sets the name for the {@link DefaultSpanEvent}
         * @param name name of the span
         * @return returns the builder
         * @since 1.2
         */
        public Builder withName(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the time for the {@link DefaultSpanEvent}
         * @param time time
         * @return returns the builder
         * @since 1.2
         */
        public Builder withTime(final String time) {
            this.time = time;
            return this;
        }

        /**
         * Optional - sets the attributes for {@link DefaultSpanEvent}. Default is an empty map.
         * @param attributes the attributes to associate with this event.
         * @return returns the builder
         * @since 1.2
         */
        public Builder withAttributes(final Map<String, Object> attributes) {
            this.attributes = attributes;
            return this;
        }

        /**
         * Optional - sets the dropped attribute count for {@link DefaultSpanEvent}. Default is 0.
         * @param droppedAttributesCount the total number of dropped attributes
         * @return returns the builder
         * @since 1.2
         */
        public Builder withDroppedAttributesCount(final Integer droppedAttributesCount) {
            this.droppedAttributesCount = droppedAttributesCount;
            return this;
        }

        /**
         * Returns a newly created {@link DefaultSpanEvent}.
         * @return a DefaultSpanEvent
         * @since 1.2
         */
        public DefaultSpanEvent build() {
            return new DefaultSpanEvent(this);
        }
    }
}
