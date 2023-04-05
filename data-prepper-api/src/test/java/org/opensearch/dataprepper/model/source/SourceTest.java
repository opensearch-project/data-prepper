/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.junit.jupiter.api.Test;

public class SourceTest implements Source<Record<Event>> {
    @Override
    public void start(Buffer<Record<Event>> buffer) {
    }

    @Override
    public void stop() {
    }

    @Test
    void testAreAcknowledgementsEnabled() {
        assertTrue(!areAcknowledgementsEnabled());
    }
}
