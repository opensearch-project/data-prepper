/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.log.JacksonLog;

class DefaultLogEventBuilder extends DefaultBaseEventBuilder<Log> implements LogEventBuilder {

    static final String LOG_EVENT_TYPE = "LOG";

    public String getEventType() {
        return LOG_EVENT_TYPE;
    }

    public Log build() {
        return (Log) JacksonLog.builder()
          .withData(getData())
          .withEventType(getEventType())
          .build();
    }
}
