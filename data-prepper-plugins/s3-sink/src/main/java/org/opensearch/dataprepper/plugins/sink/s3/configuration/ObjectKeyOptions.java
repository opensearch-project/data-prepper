/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

/**
 * An implementation class of path prefix and file pattern configuration Options
 */
public class ObjectKeyOptions {
    private static final String DEFAULT_OBJECT_NAME_PATTERN = "events-%{yyyy-MM-dd'T'HH-mm-ss'Z'}";
    private static final String DEFAULT_TIME_PATTERN = "%{yyyy-MM-dd'T'HH-mm-ss'Z'}";

    @JsonProperty("path_prefix")
    private String pathPrefix;

    @JsonProperty("name_pattern_prefix")
    private String namePatternPrefix;

    /**
     * S3 index path configuration Option
     * @return  path prefix.
     */
    public String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * S3 object name configuration Option
     * @return  S3 object prefix.
     */
    public String getNamePatternPrefix() {return namePatternPrefix;}

    /**
     * Read s3 object index file pattern configuration.
     * @return default object name pattern if namePatternPrefix is null, empty, or blank; otherwise returns the configured namePatternPrefix with default TimePattern.
     */
    public String getNamePattern() {
        if (namePatternPrefix == null || namePatternPrefix.trim().isEmpty()) {
            return DEFAULT_OBJECT_NAME_PATTERN;
        }
        return namePatternPrefix + "-" + DEFAULT_TIME_PATTERN;
    }

    @AssertTrue(message = "Custom time pattern is not allowed in the name pattern prefix since the default time pattern will be appended.")
    boolean isTimePatternExcludedFromNamePatternPrefix() {
        if (namePatternPrefix == null || namePatternPrefix.trim().isEmpty()) {
            return true;
        }
        return !namePatternPrefix.contains("%{");
    }

}