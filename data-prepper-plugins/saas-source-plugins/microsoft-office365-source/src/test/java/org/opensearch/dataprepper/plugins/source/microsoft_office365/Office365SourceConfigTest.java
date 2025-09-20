/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Office365SourceConfigTest {
    private final String tenantId = "test-tenant-id";
    private final String clientId = "test-client-id";
    private final String clientSecret = "test-client-secret";

    private Office365SourceConfig createConfig() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("tenant_id", tenantId);

        // Set up authentication configuration
        Map<String, Object> authMap = new HashMap<>();
        Map<String, String> oauth2Map = new HashMap<>();
        oauth2Map.put("client_id", clientId);
        oauth2Map.put("client_secret", clientSecret);
        authMap.put("oauth2", oauth2Map);
        configMap.put("authentication", authMap);

        // Convert to JSON and back to create config object
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonConfig = objectMapper.writeValueAsString(configMap);
        return objectMapper.readValue(jsonConfig, Office365SourceConfig.class);
    }

    @Test
    void testGetters() throws Exception {
        Office365SourceConfig config = createConfig();

        assertEquals(tenantId, config.getTenantId());
        assertNotNull(config.getAuthenticationConfiguration());
        assertEquals(clientId, config.getAuthenticationConfiguration().getOauth2().getClientId());
        assertEquals(clientSecret, config.getAuthenticationConfiguration().getOauth2().getClientSecret());
    }

    @Test
    void testDefaultValues() throws Exception {
        Office365SourceConfig config = createConfig();

        assertFalse(config.isAcknowledgments());
        assertEquals(7, config.getNumberOfWorkers());
    }
}