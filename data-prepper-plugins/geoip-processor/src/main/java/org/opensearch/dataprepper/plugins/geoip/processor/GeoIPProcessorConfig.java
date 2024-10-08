/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * An implementation class of GeoIP Processor configuration
 */
@JsonPropertyOrder
@JsonClassDescription("The <code>geoip</code> processor enriches events with geographic information extracted from IP addresses " +
        "contained in the events.")
public class GeoIPProcessorConfig {

    @Valid
    @NotNull
    @Size(min = 1)
    @JsonProperty("entries")
    @JsonPropertyDescription("The list of entries marked for enrichment.")
    private List<EntryConfig> entries;

    @JsonProperty("tags_on_engine_failure")
    @JsonPropertyDescription("The tags to add to the event metadata if the <code>geoip</code> processor is unable to enrich an event due to an engine failure.")
    private List<String> tagsOnEngineFailure;

    @JsonProperty("tags_on_ip_not_found")
    @JsonPropertyDescription("The tags to add to the event metadata if the <code>geoip</code> processor is unable to find a location for a valid IP address.")
    private List<String> tagsOnIPNotFound;

    @JsonProperty("tags_on_no_valid_ip")
    @JsonPropertyDescription("The tags to add to the event metadata if the source field is not a valid IP address. A source field may not be valid because it is incorrectly formatted or is the loopback/localhost IP address.")
    private List<String> tagsOnNoValidIp;

    @JsonProperty("geoip_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>'/srcaddr != \"8.8.8.8\"'</code>. " +
            "If specified, the <code>geoip</code> processor will only run on events when the expression evaluates to true. ")
    private String whenCondition;

    /**
     * Get List of entries
     * @return List of EntryConfig
     */
    public List<EntryConfig> getEntries() {
        return entries;
    }

    /**
     * Get the List of engine failure tags
     * @return List of engine failure tags
     */
    public List<String> getTagsOnEngineFailure() {
        return tagsOnEngineFailure;
    }

    /**
     * Get the List of IP not found in database tags
     * @return List of IP not found in database tags
     */
    public List<String> getTagsOnIPNotFound() {
        return tagsOnIPNotFound;
    }

    /**
     * Gets the list of tags to apply when a field is not a valid IP address.
     * @return List of tags
     */
    public List<String> getTagsOnNoValidIp() {
        return tagsOnNoValidIp;
    }

    /**
     * Get when condition
     * @return String When condition
     */
    public String getWhenCondition() {
        return whenCondition;
    }
}
