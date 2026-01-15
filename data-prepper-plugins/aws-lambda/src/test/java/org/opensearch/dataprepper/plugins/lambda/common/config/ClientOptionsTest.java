package org.opensearch.dataprepper.plugins.lambda.common.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientOptionsTest {

    @Test
    void testDefaultReadTimeout() {
        ClientOptions clientOptions = new ClientOptions();
        assertEquals(null, clientOptions.getReadTimeout());
    }

    @Test
    void testDefaultApiCallAttemptTimeout() {
        ClientOptions clientOptions = new ClientOptions();
        assertEquals(null, clientOptions.getApiCallAttemptTimeout());
    }
}
