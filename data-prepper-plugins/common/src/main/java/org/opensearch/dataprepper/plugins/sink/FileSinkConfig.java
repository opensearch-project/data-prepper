/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class FileSinkConfig {
    @JsonProperty("path")
    @NotEmpty
    private String path = "src/resources/file-test-sample-output.txt";

    public String getPath() {
        return path;
    }
}
