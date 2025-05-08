package org.opensearch.dataprepper.plugins.source.crowdstrike;

import org.junit.jupiter.api.Test;
import java.net.MalformedURLException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.CrowdStrikeNextLinkValidator.validateAndSanitizeURL;

public class CrowdStrikeNextLinkValidatorTest {

    @Test
    void testValidEncodedCrowdStrikeUrlPreserved() throws MalformedURLException {
        String url = "https://api.crowdstrike.com/intel/combined/indicators/v1" +
                "?filter=last_updated%3A%3E%3D1745519529%2Blast_updated%3A%3C1745523129%2B_marker%3A%3C%2717455225567d09efadf14547a1aee2bc25cabc525e%27" +
                "&limit=10000";

        String sanitized = validateAndSanitizeURL(url);

        assertTrue(sanitized.contains("filter="), "Filter parameter should be retained");
        assertTrue(sanitized.contains("last_updated%3A%3E%3D1745519529"), "Start last_updated should be present");
        assertTrue(sanitized.contains("last_updated%3A%3C1745523129"), "End last_updated should be present");
        assertTrue(sanitized.contains("_marker%3A%3C%2717455225567d09efadf14547a1aee2bc25cabc525e%27"), "_marker should be present");
        assertTrue(sanitized.contains("limit=10000"), "Limit should be present");
    }

    @Test
    void testLimitOutOfRangeExcluded() throws MalformedURLException {
        String url = "https://api.crowdstrike.com/intel/combined/indicators/v1?limit=1000000";
        String sanitized = validateAndSanitizeURL(url);
        assertFalse(sanitized.contains("limit="));
    }

    @Test
    void testXSSInjectionBlocked() throws MalformedURLException {
        String url = "https://api.crowdstrike.com/intel/combined/indicators/v1?filter=last_updated:>=1745000000+_marker:<%27<script>alert(1)</script>%27";
        String sanitized = validateAndSanitizeURL(url);
        assertFalse(sanitized.contains("<script>"));
        assertFalse(sanitized.contains("alert"));
        assertFalse(sanitized.contains("_marker")); // full subfilter should be dropped
    }

    @Test
    void testInvalidFilterFormatDropped() throws MalformedURLException {
        String url = "https://api.crowdstrike.com/intel/combined/indicators/v1?filter=last_updated===1745000000";
        String sanitized = validateAndSanitizeURL(url);
        assertFalse(sanitized.contains("filter="));
    }

    @Test
    void testJunkFilterComponentDropped() throws MalformedURLException {
        String url = "https://api.crowdstrike.com/intel/combined/indicators/v1?filter=foo:bar%2Blast_updated:>=1745000000%2B_marker:<%27123abc%27";
        String sanitized = validateAndSanitizeURL(url);
        assertFalse(sanitized.contains("foo:bar"), "Junk filter component should be dropped");
        assertTrue(sanitized.contains("last_updated"), "Valid filter 'last_updated' should be retained");
        assertTrue(sanitized.contains("_marker"), "Valid filter '_marker' should be retained");
    }

    @Test
    void testEmptyQueryPreserved() throws MalformedURLException {
        String url = "https://api.crowdstrike.com/intel/combined/indicators/v1";
        String sanitized = validateAndSanitizeURL(url);
        assertEquals(url, sanitized);
    }

}
