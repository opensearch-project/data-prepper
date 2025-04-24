package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrowdStrikeApiResponseTest {

    @Test
    void testSetAndGetBody() {
        CrowdStrikeIndicatorResult mockResult = new CrowdStrikeIndicatorResult();
        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse();
        response.setBody(mockResult);

        assertEquals(mockResult, response.getBody());
    }

    @Test
    void testGetHeader_existingHeader() {
        Map<String, List<String>> headers = Map.of(
                "X-Rate-Limit", List.of("100", "200"),
                "Content-Type", List.of("application/json")
        );

        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse();
        response.setHeaders(headers);

        List<String> values = response.getHeader("X-Rate-Limit");
        assertEquals(List.of("100", "200"), values);
    }

    @Test
    void testGetHeader_nonExistingHeaderReturnsEmptyList() {
        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse();
        response.setHeaders(Collections.emptyMap());

        List<String> result = response.getHeader("Missing");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetFirstHeaderValue_existing() {
        Map<String, List<String>> headers = Map.of(
                "Content-Type", List.of("application/json", "charset=utf-8")
        );

        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse();
        response.setHeaders(headers);

        String first = response.getFirstHeaderValue("Content-Type");
        assertEquals("application/json", first);
    }

    @Test
    void testGetFirstHeaderValue_emptyList() {
        Map<String, List<String>> headers = Map.of(
                "X-Empty", List.of()
        );

        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse();
        response.setHeaders(headers);

        assertNull(response.getFirstHeaderValue("X-Empty"));
    }

    @Test
    void testGetFirstHeaderValue_nonExistingHeader() {
        CrowdStrikeApiResponse response = new CrowdStrikeApiResponse();
        response.setHeaders(Collections.emptyMap());

        assertNull(response.getFirstHeaderValue("Not-There"));
    }
}
