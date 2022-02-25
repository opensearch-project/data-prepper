/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loggenerator;

import com.amazon.dataprepper.model.event.Event;

public interface LogTypeGenerator {
    Event generateEvent();
}
