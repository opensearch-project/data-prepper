package org.opensearch.dataprepper.plugins.source.opensearchapi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OpenSearchAPISourceConfigTest {
    @Test
    void testDefault() {
        // Prepare
        final OpenSearchAPISourceConfig sourceConfig = new OpenSearchAPISourceConfig();

        // When/Then
        assertEquals(OpenSearchAPISourceConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(OpenSearchAPISourceConfig.DEFAULT_ENDPOINT_URI, sourceConfig.getPath());
    }
}
