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
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.time.Duration;
import java.util.List;

/**
 * Represents a single processing condition entry under a scan bucket's
 * {@code processing_conditions} list. Before an S3 object is processed, the
 * worker downloads {@link #objectName} from the same S3 directory and evaluates
 * {@link #when} against its JSON content. Processing is deferred when the
 * condition is not yet satisfied.
 */
public class S3ScanProcessingCondition {

    @JsonProperty("object_name")
    @NotEmpty
    private String objectName;

    @JsonProperty("when")
    @NotEmpty
    private String when;

    @JsonProperty("applicable_prefix")
    private List<String> applicablePrefix;

    @JsonProperty("retry_delay")
    @DurationMin(minutes = 1)
    @DurationMax(minutes = 60)
    private Duration retryDelay = Duration.ofMinutes(5);

    @JsonProperty("max_retry")
    @Min(0)
    @Max(100)
    private int maxRetry = 10;

    @JsonProperty("codec")
    private PluginModel codec;

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public String getWhen() {
        return when;
    }

    public void setWhen(final String when) {
        this.when = when;
    }

    public List<String> getApplicablePrefix() {
        return applicablePrefix;
    }

    public void setApplicablePrefix(final List<String> applicablePrefix) {
        this.applicablePrefix = applicablePrefix;
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

    public PluginModel getCodec() {
        return codec;
    }

    public void setCodec(final PluginModel codec) {
        this.codec = codec;
    }
}
