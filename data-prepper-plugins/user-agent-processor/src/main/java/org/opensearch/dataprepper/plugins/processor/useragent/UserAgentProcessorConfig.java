/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class UserAgentProcessorConfig {

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

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public boolean getExcludeOriginal() {
        return excludeOriginal;
    }
}
