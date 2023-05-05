/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The default implementation of {@link Link}, a pointer from the current span to another span in a trace.
 *
 * @since 1.2
 */
public class DefaultLink implements Link {

    private String traceId;
    private String spanId;
    private String traceState;
    private Map<String, Object> attributes;
    private Integer droppedAttributesCount;

    // required for serialization
    DefaultLink() {}

    private DefaultLink(final Builder builder) {

        checkNotNull(builder.traceId, "traceId cannot be null");
        checkArgument(!builder.traceId.isEmpty(), "traceId cannot be an empty string");
        this.traceId = builder.traceId;

        checkNotNull(builder.spanId, "spanId cannot be null");
        checkArgument(!builder.spanId.isEmpty(), "spanId cannot be an empty String");
        this.spanId = builder.spanId;

        this.traceState = builder.traceState;

        this.attributes = builder.attributes == null ? new HashMap<>() : builder.attributes;
        this.droppedAttributesCount = builder.droppedAttributesCount == null ? 0 : builder.droppedAttributesCount;
    }

    @Override
    public String getTraceId() {
        return traceId;
    }

    @Override
    public String getSpanId() {
        return spanId;
    }

    @Override
    public String getTraceState() {
        return traceState;
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

        if (other instanceof DefaultLink) {
            final DefaultLink o = (DefaultLink) other;
            return (traceId.equals(o.traceId) && spanId.equals(o.spanId) && traceState.equals(o.traceState) && attributes.equals(o.attributes)
                    && droppedAttributesCount.equals(o.droppedAttributesCount));
        }
        return false;
    }

    /**
     * Builder for creating {@link DefaultLink}
     * @return returns new builder
     * @since 1.2
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String traceId;
        private String spanId;
        private String traceState;
        private Map<String, Object> attributes;
        private Integer droppedAttributesCount;

        /**
         * Sets the trace id for the {@link Link}
         * @param traceId trace id
         * @return returns the builder
         * @since 1.2
         */
        public Builder withTraceId(final String traceId) {
            this.traceId = traceId;
            return this;
        }

        /**
         * Sets the span id
         * @param spanId span id
         * @return returns the builder
         * @since 1.2
         */
        public Builder withSpanId(final String spanId) {
            this.spanId = spanId;
            return this;
        }

        /**
         * Sets the trace state
         * @param traceState trace state
         * @return returns the builder
         * @since 1.2
         */
        public Builder withTraceState(final String traceState) {
            this.traceState = traceState;
            return this;
        }

        /**
         * Optional - sets the attributes for {@link DefaultLink}. Default is an empty map.
         * @param attributes the attributes to associate with this event.
         * @return returns the builder
         * @since 1.2
         */
        public Builder withAttributes(final Map<String, Object> attributes) {
            this.attributes = attributes;
            return this;
        }

        /**
         * Optional - sets the dropped attribute count for {@link DefaultLink}. Default is 0.
         * @param droppedAttributesCount the total number of dropped attributes
         * @return returns the builder
         * @since 1.2
         */
        public Builder withDroppedAttributesCount(final Integer droppedAttributesCount) {
            this.droppedAttributesCount = droppedAttributesCount;
            return this;
        }

        /**
         * Returns a newly created {@link DefaultLink}.
         * @return a DefaultLink
         * @since 1.2
         */
        public DefaultLink build() {
            return new DefaultLink(this);
        }
    }
}
