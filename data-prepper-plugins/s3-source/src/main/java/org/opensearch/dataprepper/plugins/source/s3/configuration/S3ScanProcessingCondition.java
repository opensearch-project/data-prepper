/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.time.Duration;
import java.util.List;

/**
 * Represents a single processing condition entry under a scan bucket's
 * {@code processing_conditions} list. Before an S3 object is processed, the
 * worker downloads {@link #fileName} from the same S3 directory and evaluates
 * {@link #when} against its JSON content. Processing is deferred when the
 * condition is not yet satisfied.
 */
public class S3ScanProcessingCondition {

    @JsonProperty("file_name")
    @NotEmpty
    private String fileName;

    @JsonProperty("when")
    @NotEmpty
    private String when;

    @JsonProperty("include_prefix")
    private List<String> includePrefix;

    @JsonProperty("retry_delay")
    private Duration retryDelay = Duration.ofMinutes(5);

    @JsonProperty("max_retry")
    private int maxRetry = 10;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(final String fileName) {
        this.fileName = fileName;
    }

    public String getWhen() {
        return when;
    }

    public void setWhen(final String when) {
        this.when = when;
    }

    public List<String> getIncludePrefix() {
        return includePrefix;
    }

    public void setIncludePrefix(final List<String> includePrefix) {
        this.includePrefix = includePrefix;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(final Duration retryDelay) {
        this.retryDelay = retryDelay;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(final int maxRetry) {
        this.maxRetry = maxRetry;
    }
}
