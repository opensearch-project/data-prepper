package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ThreatIndicatorTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDeserializeFullJson() throws Exception {
        String json = "{\n" +
                "  \"id\": \"ioc-12345\",\n" +
                "  \"type\": \"domain\",\n" +
                "  \"indicator\": \"malicious.com\",\n" +
                "  \"published_date\": 1680000,\n" +
                "  \"last_updated\": 1680100,\n" +
                "  \"malicious_confidence\": \"high\"\n" +
                "}";

        ThreatIndicator indicator = objectMapper.readValue(json, ThreatIndicator.class);

        assertEquals("ioc-12345", indicator.getId());
        assertEquals("domain", indicator.getType());
        assertEquals("malicious.com", indicator.getIndicator());
        assertEquals(1680000L, indicator.getPublishedDate());
        assertEquals(1680100L, indicator.getLastUpdated());
        assertEquals("high", indicator.getMaliciousConfidence());
    }

    @Test
    void testDeserializeWithUnknownFields_ignored() throws Exception {
        String json = "{\n" +
                "  \"id\": \"ioc-999\",\n" +
                "  \"type\": \"ip_address\",\n" +
                "  \"indicator\": \"1.2.3.4\",\n" +
                "  \"published_date\": 1670000,\n" +
                "  \"unknown_field\": \"something-weird\"\n" +
                "}";

        ThreatIndicator indicator = objectMapper.readValue(json, ThreatIndicator.class);

        assertEquals("ioc-999", indicator.getId());
        assertEquals("ip_address", indicator.getType());
        assertEquals("1.2.3.4", indicator.getIndicator());
        assertEquals(1670000L, indicator.getPublishedDate());
    }

    @Test
    void testDeserializeMissingOptionalFields_defaultsApplied() throws Exception {
        String json = "{\n" +
                "  \"id\": \"ioc-000\",\n" +
                "  \"type\": \"url\",\n" +
                "  \"indicator\": \"http://bad.com\"\n" +
                "}";

        ThreatIndicator indicator = objectMapper.readValue(json, ThreatIndicator.class);

        assertEquals("ioc-000", indicator.getId());
        assertEquals("url", indicator.getType());
        assertEquals("http://bad.com", indicator.getIndicator());
        assertEquals(0L, indicator.getPublishedDate());
        assertEquals(0L, indicator.getLastUpdated());
        assertNull(indicator.getMaliciousConfidence());
    }
}
