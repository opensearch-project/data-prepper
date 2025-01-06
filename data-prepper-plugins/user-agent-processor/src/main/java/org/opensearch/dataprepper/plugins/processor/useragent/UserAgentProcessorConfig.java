/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>user_agent</code> processor parses any user agent (UA) string in an event and then adds the " +
        "parsed results to the event.")
public class UserAgentProcessorConfig {

    private static final int DEFAULT_CACHE_SIZE = 1000;

    @NotEmpty
    @NotNull
    @JsonProperty("source")
    @JsonPropertyDescription("The field in the event that will be parsed.")
    @EventKeyConfiguration(EventKeyFactory.EventAction.GET)
    private EventKey source;

    @NotNull
    @JsonProperty(value = "target", defaultValue = "user_agent")
    @JsonPropertyDescription("The field to which the parsed event will write. Default is <code>user_agent</code>.")
    private String target = "user_agent";

    @NotNull
    @JsonProperty("exclude_original")
    @JsonPropertyDescription("Determines whether to exclude the original UA string from the parsing result. Defaults to <code>false</code>.")
    private boolean excludeOriginal = false;

    @JsonProperty(value = "cache_size", defaultValue = "1000")
    @JsonPropertyDescription("The cache size of the parser in megabytes. Defaults to <code>1000</code>.")
    private int cacheSize = DEFAULT_CACHE_SIZE;

    @JsonProperty("tags_on_parse_failure")
    @JsonPropertyDescription("The tag to add to an event if the <code>user_agent</code> processor fails to parse the UA string.")
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
