package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Represents a threat intelligence indicator from CrowdStrike's API.
 * This class encapsulates information about potential security threats,
 * including indicators of compromise (IoCs) and associated metadata.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ThreatIndicator {
    /**
     * The ID of the IOC.
     */
    @JsonProperty("id")
    private String id = null;

    /**
     * The type of the IOC.
     */
    @JsonProperty("type")
    private String type = null;

    /**
     * The value of the IOC.
     */
    @JsonProperty("indicator")
    private String indicator = null;

    /**
     * The epoch timestamp of the creation date of IOC.
     */
    @JsonProperty("published_date")
    private long publishedDate = 0L;

    @JsonProperty("malicious_confidence")
    private String maliciousConfidence = null;

    /**
     * The epoch timestamp of last updated date of the IOC.
     */
    @JsonProperty("last_updated")
    private long lastUpdated = 0L;

}
