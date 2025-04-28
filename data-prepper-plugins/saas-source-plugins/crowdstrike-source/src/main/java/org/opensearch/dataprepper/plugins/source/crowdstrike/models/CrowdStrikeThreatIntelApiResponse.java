package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import lombok.Data;
import org.springframework.util.CollectionUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents the response returned from a CrowdStrike API call.
 */
@Data
 public class CrowdStrikeThreatIntelApiResponse {

    private CrowdStrikeIndicatorResult body;
    private Map<String, List<String>> headers;

    public CrowdStrikeThreatIntelApiResponse(CrowdStrikeIndicatorResult body, Map<String, List<String>> headers) {
        this.body = body;
        this.headers = headers;
    }

    // Convenience method to get a specific header
    public List<String> getHeader(String headerName) {
        return headers.getOrDefault(headerName, Collections.emptyList());
    }

    // Convenience method to get the first value of a specific header
    public String getFirstHeaderValue(String headerName) {
        List<String> values = getHeader(headerName);
        return CollectionUtils.isEmpty(values) ? null : values.get(0);
    }
}