/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import org.opensearch.dataprepper.http.common.HttpServerConfig;
import jakarta.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class HTTPSourceConfig extends HttpServerConfig {
    static final String DEFAULT_LOG_INGEST_URI = "/log/ingest";
    static final int DEFAULT_PORT = 2021;

    @JsonProperty("port")
    @Min(0)
    @Max(65535)
    private int port = DEFAULT_PORT;

    @JsonProperty("path")
    @Size(min = 1, message = "path length should be at least 1")
    private String path = DEFAULT_LOG_INGEST_URI;

    @AssertTrue(message = "path should start with /")
    boolean isPathValid() {
        return path.startsWith("/");
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }
}
