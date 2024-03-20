/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * An implementation class of GeoIP Processor configuration
 */
public class GeoIPProcessorConfig {

    @Valid
    @NotNull
    @Size(min = 1)
    @JsonProperty("entries")
    private List<EntryConfig> entries;

    @JsonProperty("tags_on_engine_failure")
    private List<String> tagsOnEngineFailure;

    @JsonProperty("tags_on_ip_not_found")
    private List<String> tagsOnIPNotFound;

    @JsonProperty("tags_on_no_valid_ip")
    private List<String> tagsOnNoValidIp;

    @JsonProperty("geoip_when")
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
