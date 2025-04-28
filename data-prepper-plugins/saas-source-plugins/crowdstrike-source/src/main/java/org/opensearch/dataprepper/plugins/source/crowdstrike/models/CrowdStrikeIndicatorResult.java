package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Represents the response from a CrowdStrike Falcon Query Language (FQL) search for threat indicators.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrowdStrikeIndicatorResult {

    @JsonProperty("resources")
    private List<ThreatIndicator> results = null;


}
