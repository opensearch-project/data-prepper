/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.event.DefaultBaseEventBuilder;

public class DefaultLogEventBuilder extends DefaultBaseEventBuilder<Log> implements LogEventBuilder {

    static final String LOG_EVENT_TYPE = "LOG";

    public Log build() {
        return (Log) JacksonLog.builder()
          .withData(getData())
          .withEventType(LOG_EVENT_TYPE)
          .build();
    }
}
