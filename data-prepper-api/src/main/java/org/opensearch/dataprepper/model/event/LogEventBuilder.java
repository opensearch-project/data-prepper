/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
    Log build();
}
