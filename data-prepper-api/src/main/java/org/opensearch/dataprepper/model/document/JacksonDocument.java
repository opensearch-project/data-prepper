/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.document;

import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Document}. This class extends the {@link JacksonEvent}.
 */
public class JacksonDocument extends JacksonEvent implements Document {

    protected JacksonDocument(final Builder builder) {
        super(builder);

        checkArgument(this.getMetadata().getEventType().equals("DOCUMENT"), "eventType must be of type Document");
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating {@link JacksonDocument}
     * @since 2.1
     */
    public static class Builder extends JacksonEvent.Builder<Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Returns a newly created {@link JacksonDocument}
         * @return a JacksonDocument
         * @since 2.1
         */
        public JacksonDocument build() {
            this.withEventType(EventType.DOCUMENT.toString());
            return new JacksonDocument(this);
        }
    }
}
