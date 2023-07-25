package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;

import java.io.IOException;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OpenSearchDefaultBulkApiWrapperTest {
    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private BulkRequest bulkRequest;

    private OpenSearchDefaultBulkApiWrapper objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new OpenSearchDefaultBulkApiWrapper(openSearchClient);
    }

    @Test
    void testBulk() throws IOException {
        objectUnderTest.bulk(bulkRequest);
        verify(openSearchClient).bulk(bulkRequest);
    }
}