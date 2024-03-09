/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.log.Log;
import org.springframework.stereotype.Component;

@Component
class DefaultLogEventBuilderFactory extends DefaultEventBuilderFactory {
    static final String LOG_EVENT_TYPE = "LOG";

    Class<?> getEventClass() {
        return LogEventBuilder.class;
    }

    DefaultBaseEventBuilder createNew() {
        return new DefaultLogEventBuilder();
    }

    public static class DefaultLogEventBuilder extends DefaultBaseEventBuilder<Log> implements LogEventBuilder {
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
}
