/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
