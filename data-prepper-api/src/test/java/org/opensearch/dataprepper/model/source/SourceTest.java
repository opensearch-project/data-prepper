/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SourceTest {
    @Test
    void testAreAcknowledgementsEnabled() {
      Source<Record<Event>> objectUnderTest = mock(Source.class);
      when(objectUnderTest.areAcknowledgementsEnabled()).thenCallRealMethod();
      assertFalse(objectUnderTest.areAcknowledgementsEnabled());
    }
}
