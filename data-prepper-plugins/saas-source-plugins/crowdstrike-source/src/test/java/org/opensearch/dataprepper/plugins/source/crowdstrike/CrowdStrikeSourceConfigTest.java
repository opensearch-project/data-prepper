package org.opensearch.dataprepper.plugins.source.crowdstrike;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CrowdStrikeSourceConfigTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private CrowdStrikeSourceConfig config;

    private static final int DEFAULT_WORKERS = 5;

    @BeforeEach
    void setup() {
        config = new CrowdStrikeSourceConfig();
    }

    @Test
    void testDefaultValues() {
        assertEquals(DEFAULT_WORKERS, config.getNumWorkers());
        assertFalse(config.isAcknowledgments());
    }

    @Test
    void testDeserializationWithValues() throws Exception {
        Map<String, Object> yamlConfig = new HashMap<>();
        Map<String, Object> authMap = new HashMap<>();
        authMap.put("client_id", "dummy-client-id");
        authMap.put("client_secret", "dummy-client-secret");
        yamlConfig.put("authentication", authMap);
        yamlConfig.put("acknowledgments", true);
        yamlConfig.put("workers", 10);

        String json = objectMapper.writeValueAsString(yamlConfig);
        CrowdStrikeSourceConfig loadedConfig = objectMapper.readValue(json, CrowdStrikeSourceConfig.class);

        assertNotNull(loadedConfig.getAuthenticationConfig());
        assertTrue(loadedConfig.isAcknowledgments());
        assertEquals(10, loadedConfig.getNumWorkers());
    }

}
