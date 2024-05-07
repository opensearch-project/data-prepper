/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.write_json;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class WriteJsonProcessorConfig {
    @JsonProperty("source")
    @NotNull
    private String source;

    @JsonProperty("target")
    private String target;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }
}
