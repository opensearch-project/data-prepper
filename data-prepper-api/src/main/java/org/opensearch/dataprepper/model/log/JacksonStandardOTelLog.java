/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.log;

import org.opensearch.dataprepper.model.event.JacksonEvent;

/**
 * A Jackson Standard implementation for {@link OpenTelemetryLog}.
 *
 * @since 2.11
 */
public class JacksonStandardOTelLog extends JacksonOtelLog {

    JacksonStandardOTelLog(final JacksonOtelLog.Builder builder) {
        super(builder);
    }


    @Override
    public String toJsonString() {
        return ((JacksonEvent)this).toJsonString();
    }
}
