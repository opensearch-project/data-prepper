/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;

public class UserAgentProcessorConfig {

    private static final int DEFAULT_CACHE_SIZE = 1000;

    @NotEmpty
    @NotNull
    @JsonProperty("source")
    @JsonPropertyDescription("The field in the event that will be parsed.")
    @EventKeyConfiguration(EventKeyFactory.EventAction.GET)
    private EventKey source;

    @NotNull
    @JsonProperty("target")
    @JsonPropertyDescription("The field to which the parsed event will write. Default is `user_agent`.")
    private String target = "user_agent";

    @NotNull
    @JsonProperty("exclude_original")
    @JsonPropertyDescription("Determines whether to exclude the original UA string from the parsing result. Defaults to `false`. ")
    private boolean excludeOriginal = false;

    @JsonProperty("cache_size")
    @JsonPropertyDescription("The cache size of the parser in megabytes. Defaults to `1000`.")
    private int cacheSize = DEFAULT_CACHE_SIZE;

    @JsonProperty("tags_on_parse_failure")
    @JsonPropertyDescription("The tag to add to an event if the `user_agent` processor fails to parse the UA string.")
    private List<String> tagsOnParseFailure;

    public EventKey getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public boolean getExcludeOriginal() {
        return excludeOriginal;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public List<String> getTagsOnParseFailure() {
        return tagsOnParseFailure;
    }
}
