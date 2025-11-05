/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class DataStreamDetectorSimpleTest {

    @Mock
    private OpenSearchClient openSearchClient;

    private DataStreamDetector dataStreamDetector;

    @BeforeEach
    void setUp() {
        dataStreamDetector = new DataStreamDetector(openSearchClient);
    }

    @Test
    void constructor_createsInstance() {
        assertNotNull(dataStreamDetector);
    }

    @Test
    void isDataStream_returnsFalse_whenClientThrowsException() {
        // When OpenSearch client throws exception, should return false
        assertFalse(dataStreamDetector.isDataStream("test-index"));
    }

    @Test
    void clearCache_doesNotThrowException() {
        dataStreamDetector.clearCache();
        // Should not throw any exception
    }
}