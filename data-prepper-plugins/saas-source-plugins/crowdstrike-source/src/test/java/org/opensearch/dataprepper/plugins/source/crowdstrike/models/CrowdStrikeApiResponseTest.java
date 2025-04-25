package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrowdStrikeApiResponseTest {
    @Mock
    CrowdStrikeIndicatorResult mockResult;
    Map<String, List<String>> headers = Map.of(
            "X-Rate-Limit", List.of("100", "200"),
            "Content-Type", List.of("application/json")
    );

    @Test
    void testSetAndGetBody() {
        ThreatIndicator indicator1 = new ThreatIndicator();
        indicator1.setId("ioc-001");
        indicator1.setType("domain");
        indicator1.setIndicator("malicious.com");
        indicator1.setPublishedDate(1680000000L);
        indicator1.setLastUpdated(1680001234L);

        ThreatIndicator indicator2 = new ThreatIndicator();
        indicator2.setId("ioc-002");
        indicator2.setType("ip_address");
        indicator2.setIndicator("1.2.3.4");
        indicator2.setPublishedDate(1681111111L);
        indicator2.setLastUpdated(1681112222L);

        // Set them into a CrowdStrikeIndicatorResult
        CrowdStrikeIndicatorResult result = new CrowdStrikeIndicatorResult();
        result.setResults(List.of(indicator1, indicator2));

        // Create response
        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse(result, headers);
        response.setBody(result);

        // Verify body content
        assertNotNull(response.getBody());
        assertEquals(2, response.getBody().getResults().size());
        assertEquals("ioc-001", response.getBody().getResults().get(0).getId());
        assertEquals("ioc-002", response.getBody().getResults().get(1).getId());
    }

    @Test
    void testGetHeader_existingHeader() {
        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse(mockResult, headers);
        response.setHeaders(headers);

        List<String> values = response.getHeader("X-Rate-Limit");
        assertEquals(List.of("100", "200"), values);
    }

    @Test
    void testGetHeader_nonExistingHeaderReturnsEmptyList() {
        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse(mockResult, Collections.emptyMap());
        List<String> result = response.getHeader("Missing");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetFirstHeaderValue_existing() {
        Map<String, List<String>> headers = Map.of(
                "Content-Type", List.of("application/json", "charset=utf-8")
        );

        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse(mockResult, headers);
        response.setHeaders(headers);

        String first = response.getFirstHeaderValue("Content-Type");
        assertEquals("application/json", first);
    }

    @Test
    void testGetFirstHeaderValue_emptyList() {
        Map<String, List<String>> headers = Map.of(
                "X-Empty", List.of()
        );

        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse(mockResult, headers);
        response.setHeaders(headers);

        assertNull(response.getFirstHeaderValue("X-Empty"));
    }

}
