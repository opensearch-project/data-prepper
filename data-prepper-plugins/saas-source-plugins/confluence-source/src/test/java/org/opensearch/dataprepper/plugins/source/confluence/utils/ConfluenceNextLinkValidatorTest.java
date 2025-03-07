package org.opensearch.dataprepper.plugins.source.confluence.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfluenceNextLinkValidatorTest {

    @Test
    void testValidURL() throws Exception {
        String validUrl = "http://hostname/rest/api/content/search?next=true&limit=25&start=25";
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(validUrl);
        assertNotNull(sanitized);
        assertTrue(sanitized.contains("next=true"));
        assertTrue(sanitized.contains("limit=25"));
        assertTrue(sanitized.contains("start=25"));
    }

    @Test
    void testComplexValidURL() throws Exception {
        String validUrl = "http://hostname/rest/api/content/search?next=true&cursor=_f_MjU%3D_sa_WyJcdDUxNTk1NDM5IHIhXCJgKEM%2BOz1bT2UpR0dpQ3BIKiBjcCJd&limit=25&start=25&cql=lastModified%3E=%222024-02-25%2010:50%22%20AND%20space%20in%20(%22SD%22)";
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(validUrl);
        assertNotNull(sanitized);
        assertTrue(sanitized.contains("cursor="));
        assertTrue(sanitized.contains("cql="));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://hostname/api?next=invalid",
            "http://hostname/api?limit=abc",
            "http://hostname/api?start=abc",
            "http://hostname/api?unknown=value"
    })
    void testInvalidParameters(String invalidUrl) throws Exception {
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(invalidUrl);
        assertFalse(sanitized.endsWith("invalid"));
        assertFalse(sanitized.contains("unknown=value"));
    }

    @Test
    void testMalformedURL() {
        assertThrows(MalformedURLException.class, () ->
                ConfluenceNextLinkValidator.validateAndSanitizeURL("not-a-url")
        );
    }

    @Test
    void testNullURL() {
        assertThrows(MalformedURLException.class, () ->
                ConfluenceNextLinkValidator.validateAndSanitizeURL(null)
        );
    }

    @Test
    void testEmptyParameters() throws Exception {
        String url = "http://hostname/api?";
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(url);
        assertEquals("http://hostname/api?", sanitized);
    }

    @Test
    void testUrlWithInjectionAttempts() throws Exception {
        String maliciousUrl = "http://hostname/api?next=true&evil=<script>alert(1)</script>&limit=25";
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(maliciousUrl);
        assertFalse(sanitized.contains("<script>"));
        assertFalse(sanitized.contains("evil="));
    }


    @Test
    void testCQLParameterValidation() throws Exception {
        // Valid CQL
        String validCql = "lastModified>=\"2024-02-25 10:50\" AND space in (\"SD\")";
        String url = "http://hostname/api?cql=" + URLEncoder.encode(validCql, StandardCharsets.UTF_8);
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(url);
        assertTrue(sanitized.contains("cql="));

        // Invalid CQL with SQL injection attempt
        String invalidCql = "lastModified>=\"2024-02-25\"; DROP TABLE users;";
        url = "http://hostname/api?cql=" + URLEncoder.encode(invalidCql, StandardCharsets.UTF_8);
        sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(url);
        assertFalse(sanitized.contains("DROP TABLE"));
    }

    @Test
    void testCursorParameterValidation() throws Exception {
        // Valid cursor
        assertTrue(ConfluenceNextLinkValidator.validateAndSanitizeURL("http://hostname/api?cursor=abc123_ABC")
                .contains("cursor=abc123_ABC"));

        // Invalid cursor
        assertFalse(ConfluenceNextLinkValidator.validateAndSanitizeURL("http://hostname/api?cursor=<>\"'")
                .contains("<>\"'"));
    }

    @Test
    void testMultipleIdenticalParameters() throws Exception {
        String url = "http://hostname/api?limit=25&limit=50";
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(url);
        // Verify only one limit parameter is preserved
        assertEquals(1, sanitized.split("limit=").length - 1);
    }

    @Test
    void testURLEncoding() throws Exception {
        String url = "http://hostname/api?cql=space=\"My Space\"";
        String sanitized = ConfluenceNextLinkValidator.validateAndSanitizeURL(url);
        assertTrue(sanitized.contains(URLEncoder.encode("\"", StandardCharsets.UTF_8)));
    }
}

