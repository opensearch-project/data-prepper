/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.plugins.processor.configuration.EntryConfig;

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

    @JsonProperty("tags_on_failure")
    private List<String> tagsOnFailure;

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
     * Get the List of failure tags
     * @return List of failure tags
     */
    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    /**
     * Get when condition
     * @return String When condition
     */
    public String getWhenCondition() {
        return whenCondition;
    }
}
