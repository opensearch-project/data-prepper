/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Class consists the scan include and exclude keys properties.
 */
public class S3ScanKeyPathOption {
    @JsonProperty("include")
    private List<String> s3scanIncludeOptions;
    @JsonProperty("exclude_suffix")
    private List<String> s3ScanExcludeSuffixOptions;

    public List<String> getS3scanIncludeOptions() {
        return s3scanIncludeOptions;
    }

    public List<String> getS3ScanExcludeSuffixOptions() {
        return s3ScanExcludeSuffixOptions;
    }
}