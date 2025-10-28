/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

public interface SinkBufferEntryProvider {
    SinkBufferEntry getSinkBufferEntry(final Event event) throws Exception;
}

