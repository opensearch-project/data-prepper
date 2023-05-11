/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.log.Log;

public interface LogEventBuilder extends BaseEventBuilder<Log> {
    /**
     * Returns a newly created {@link JacksonLog}.
     *
     * @return a log event
     * @since 2.2
     */
    public Log build();
}
