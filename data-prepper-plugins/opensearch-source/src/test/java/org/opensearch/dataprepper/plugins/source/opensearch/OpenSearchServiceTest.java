/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class OpenSearchServiceTest {

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private SearchAccessor searchAccessor;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;

    private OpenSearchService createObjectUnderTest() {
        return OpenSearchService.createOpenSearchService(searchAccessor, sourceCoordinator, openSearchSourceConfiguration, buffer);
    }

    @Test
    void source_coordinator_is_initialized_on_construction() {
        createObjectUnderTest();
        verify(sourceCoordinator).initialize();
    }
}
