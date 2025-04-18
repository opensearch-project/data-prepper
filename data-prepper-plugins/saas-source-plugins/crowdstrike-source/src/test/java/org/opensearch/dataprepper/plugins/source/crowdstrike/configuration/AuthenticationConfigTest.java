package org.opensearch.dataprepper.plugins.source.crowdstrike.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthenticationConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testValidConfigWithClientIdAndSecret() throws JsonProcessingException {
        Map<String, Object> yamlConfig = new HashMap<>();
        yamlConfig.put("client_id", "dummy-client-id");
        yamlConfig.put("client_secret", "dummy-client-secret");

        String json = objectMapper.writeValueAsString(yamlConfig);
        AuthenticationConfig loadedConfig = objectMapper.readValue(json, AuthenticationConfig.class);

        assertNotNull(loadedConfig.getClientId());
        assertEquals("dummy-client-id", loadedConfig.getClientId());
        assertEquals("dummy-client-secret", loadedConfig.getClientSecret());
        assertTrue(loadedConfig.isValidConfig(), "Expected config to be valid when both clientId and clientSecret are set");
    }

    @Test
    void testInvalidConfigWithMissingClientId() throws JsonProcessingException {
        Map<String, Object> yamlConfig = new HashMap<>();
        yamlConfig.put("client_id", "dummy-client-id");
        String json = objectMapper.writeValueAsString(yamlConfig);
        AuthenticationConfig loadedConfig = objectMapper.readValue(json, AuthenticationConfig.class);
        assertFalse(loadedConfig.isValidConfig(), "Expected config to be invalid when clientId is missing");
    }


    @Test
    void testInvalidConfigWithNoCredentials() {
        AuthenticationConfig config = new AuthenticationConfig();
        assertFalse(config.isValidConfig(), "Expected config to be invalid when both clientId and clientSecret are missing");
    }
}
