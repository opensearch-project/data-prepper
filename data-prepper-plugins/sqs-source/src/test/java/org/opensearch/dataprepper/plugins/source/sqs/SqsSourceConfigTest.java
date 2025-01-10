/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SqsSourceConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDefaultValues() {
        final SqsSourceConfig config = new SqsSourceConfig();
        assertNull(config.getAwsAuthenticationOptions(), "AWS Authentication Options should be null by default");
        assertFalse(config.getAcknowledgements(), "Acknowledgments should be false by default");
        assertEquals(SqsSourceConfig.DEFAULT_BUFFER_TIMEOUT, config.getBufferTimeout(), "Buffer timeout should default to 10 seconds");
        assertNull(config.getQueues(), "Queues should be null by default");
    }
}