package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrowdStrikeIndicatorResultTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDeserializeWithResources() throws Exception {
        String json = "{\n" +
                "  \"resources\": [\n" +
                "    {\n" +
                "      \"type\": \"ip_address\",\n" +
                "      \"indicator\": \"1.2.3.4\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"domain\",\n" +
                "      \"indicator\": \"malicious.com\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";


        CrowdStrikeIndicatorResult result = objectMapper.readValue(json, CrowdStrikeIndicatorResult.class);

        assertNotNull(result.getResults());
        assertEquals(2, result.getResults().size());
        assertEquals("ip_address", result.getResults().get(0).getType());
        assertEquals("1.2.3.4", result.getResults().get(0).getIndicator());
        assertEquals("domain", result.getResults().get(1).getType());
        assertEquals("malicious.com", result.getResults().get(1).getIndicator());

    }

    @Test
    void testDeserializeWithUnknownFields_ignored() throws Exception {
        String json = "{\n" +
                "  \"resources\": [],\n" +
                "  \"unexpected_field\": \"ignore_me\"\n" +
                "}";

        CrowdStrikeIndicatorResult result = objectMapper.readValue(json, CrowdStrikeIndicatorResult.class);

        assertNotNull(result.getResults());
        assertTrue(result.getResults().isEmpty());
    }

    @Test
    void testEmptyResources_defaultsToNull() {
        CrowdStrikeIndicatorResult result = new CrowdStrikeIndicatorResult();
        assertNull(result.getResults());
    }
}
