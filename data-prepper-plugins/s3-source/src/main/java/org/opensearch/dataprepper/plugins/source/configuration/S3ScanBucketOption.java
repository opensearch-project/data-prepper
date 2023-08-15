/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.plugins.source.CustomLocalDateTimeDeserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Class consists the bucket related configuration properties.
 */
public class S3ScanBucketOption {
    private static final String S3_PREFIX = "s3://";

    @JsonProperty("name")
    @NotEmpty
    @Size(min = 3, max = 500, message = "bucket length should be at least 3 characters")
    private String name;

    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    @JsonProperty("start_time")
    private LocalDateTime startTime;

    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    @JsonProperty("end_time")
    private LocalDateTime endTime;

    @JsonProperty("range")
    private Duration range;

    @JsonProperty("filter")
    private S3ScanKeyPathOption s3ScanFilter;

    @AssertTrue(message = "At most two options from start_time, end_time and range can be specified at the same time")
    public boolean hasValidTimeOptions() {
        return Stream.of(startTime, endTime, range).filter(Objects::nonNull).count() < 3;
    }

    public String getName() {
        if (name.startsWith(S3_PREFIX)) {
            return name.substring(S3_PREFIX.length());
        }
        return name;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public Duration getRange() {
        return range;
    }

    public S3ScanKeyPathOption getS3ScanFilter() {
        return s3ScanFilter;
    }
}