/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.log;

import org.opensearch.dataprepper.model.event.Event;

/**
 * A log event in Data Prepper represents a single log line. A log event does not require any specific keys allowing this event type to
 * support any log structure.
 * @since 1.2
 */
public interface Log extends Event {

}
