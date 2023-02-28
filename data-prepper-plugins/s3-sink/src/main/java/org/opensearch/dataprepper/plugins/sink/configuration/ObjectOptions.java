/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/*
    An implementation class of Threshold configuration Options
 */
public class ObjectOptions {
    private static final String DEFAULT_KEY_PATTERN = "logs-${YYYY-MM-DD hh:mm:ss}";

    @JsonProperty("file_pattern")
    @NotNull
    private String filePattern = DEFAULT_KEY_PATTERN;

    /*
        Read s3 object index file patten configuration
     */
    public String getFilePattern() {
        return filePattern;
    }
}
