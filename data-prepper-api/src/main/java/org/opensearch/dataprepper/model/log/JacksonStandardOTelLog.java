/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.log;

/**
 * A Jackson Standard implementation for {@link OpenTelemetryLog}.
 *
 * @since 2.11
 */
public class JacksonStandardOTelLog extends JacksonOtelLog {

    JacksonStandardOTelLog(final JacksonOtelLog.Builder builder) {
        super(builder);
    }

    public static JacksonStandardOTelLog.Builder builder() {
        return new JacksonStandardOTelLog.Builder();
    }

    @Override
    public String toJsonString() {
        return getJsonNode().toString();
    }

    public static class Builder extends JacksonOtelLog.Builder {

        @Override
        public JacksonOtelLog build() {
            populateEvent();
            return new JacksonStandardOTelLog(this);
        }

    }
}
