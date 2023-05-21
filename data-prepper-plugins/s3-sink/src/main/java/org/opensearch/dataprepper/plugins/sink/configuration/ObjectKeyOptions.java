/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An implementation class of path prefix and file pattern configuration Options
 */
public class ObjectKeyOptions {
    private static final String DEFAULT_OBJECT_NAME_PATTERN = "events-%{yyyy-MM-dd'T'hh-mm-ss}";

    @JsonProperty("path_prefix")
    private String pathPrefix;

    /**
     * S3 index path configuration Option
     * @return  path prefix.
     */
    public String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * Read s3 object index file pattern configuration.
     * @return default object name pattern.
     */
    public String getNamePattern() {
        return DEFAULT_OBJECT_NAME_PATTERN;
    }
}