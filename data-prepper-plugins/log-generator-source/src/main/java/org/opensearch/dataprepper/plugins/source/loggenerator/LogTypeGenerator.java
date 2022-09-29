/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;

import org.opensearch.dataprepper.model.event.Event;

public interface LogTypeGenerator {
    Event generateEvent();
}
