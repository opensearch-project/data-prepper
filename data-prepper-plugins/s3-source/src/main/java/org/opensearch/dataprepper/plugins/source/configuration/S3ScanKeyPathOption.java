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
    @JsonProperty("include_prefix")
    private List<String> s3scanIncludePrefixOptions;
    @JsonProperty("exclude_suffix")
    private List<String> s3ScanExcludeSuffixOptions;

    public List<String> getS3scanIncludePrefixOptions() {
        return s3scanIncludePrefixOptions;
    }

    public List<String> getS3ScanExcludeSuffixOptions() {
        return s3ScanExcludeSuffixOptions;
    }
}