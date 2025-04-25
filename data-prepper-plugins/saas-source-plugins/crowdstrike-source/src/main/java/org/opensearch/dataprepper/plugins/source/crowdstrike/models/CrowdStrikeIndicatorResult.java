package org.opensearch.dataprepper.plugins.source.crowdstrike.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * The result of Falcon query search.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrowdStrikeIndicatorResult {

    @JsonProperty("resources")
    private List<ThreatIndicator> results = null;


}
