/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class UserAgentProcessorConfig {

    private static final int DEFAULT_CACHE_SIZE = 1000;

    @NotEmpty
    @NotNull
    @JsonProperty("source")
    private String source;

    @NotNull
    @JsonProperty("target")
    private String target = "user_agent";

    @NotNull
    @JsonProperty("exclude_original")
    private boolean excludeOriginal = false;

    @JsonProperty("cache_size")
    private int cacheSize = DEFAULT_CACHE_SIZE;

    @JsonProperty("tags_on_parse_failure")
    private List<String> tagsOnParseFailure;

    public String getSource() {
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
