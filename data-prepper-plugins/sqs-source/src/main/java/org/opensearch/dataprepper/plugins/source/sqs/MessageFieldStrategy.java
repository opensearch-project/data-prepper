/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.event.Event;
import java.util.List;

public interface MessageFieldStrategy {
  /**
   * Converts the SQS message body into one or more events.
   */
    List<Event> parseEvents(String messageBody);
}
