/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.plugins.source.CustomLocalDateTimeDeserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Class consists the bucket related configuration properties.
 */
public class S3ScanBucketOption {
    @JsonProperty("name")
    private String name;

    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    @JsonProperty("start_time")
    private LocalDateTime startTime;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonProperty("end_time")
    private LocalDateTime endTime;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonProperty("range")
    private Duration range;

    @JsonProperty("filter")
    private S3ScanKeyPathOption s3ScanFilter;

    @AssertTrue(message = "At most two options from start_time, end_time and range can be specified at the same time")
    public boolean hasValidTimeOptions() {
        return Stream.of(startTime, endTime, range).filter(Objects::nonNull).count() < 3;
    }

    public String getName() {
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