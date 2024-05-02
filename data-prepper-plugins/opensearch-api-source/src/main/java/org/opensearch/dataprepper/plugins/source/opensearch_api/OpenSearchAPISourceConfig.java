/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch_api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.opensearch.dataprepper.http.common.HttpServerConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OpenSearchAPISourceConfig extends HttpServerConfig {
    static final String DEFAULT_LOG_INGEST_URI = "/";
    static final int DEFAULT_PORT = 9202;

    @JsonProperty("port")
    @Min(0)
    @Max(65535)
    private int port = DEFAULT_PORT;

    @JsonProperty("path_prefix")
    @Size(min = 1, message = "path length should be at least 1")
    private String path_prefix = DEFAULT_LOG_INGEST_URI;

    @AssertTrue(message = "path should start with /")
    boolean isPathValid() {
        return path_prefix.startsWith("/");
    }

    @JsonProperty("acknowledgments")
    private Boolean acknowledgments = false;

    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationOptions awsConfig;

    @JsonProperty("delete_s3_objects_on_read")
    private boolean deleteS3ObjectsOnRead = false;

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path_prefix;
    }
}
