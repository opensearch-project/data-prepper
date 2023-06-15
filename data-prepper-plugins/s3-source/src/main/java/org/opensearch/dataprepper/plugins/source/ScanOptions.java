/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Class consists the scan related properties.
 */
public class ScanOptions {

    private LocalDateTime startDateTime;

    private Duration range;

    private S3ScanBucketOption bucketOption;

    private LocalDateTime endDateTime;

    private LocalDateTime useStartDateTime;

    private LocalDateTime useEndDateTime;

    private ScanOptions(Builder builder){
        this.startDateTime = builder.startDateTime;
        this.range = builder.range;
        this.bucketOption = builder.bucketOption;
        this.endDateTime = builder.endDateTime;
        this.useStartDateTime = builder.useStartDateTime;
        this.useEndDateTime = builder.useEndDateTime;
    }

    public S3ScanBucketOption getBucketOption() {
        return bucketOption;
    }

    public LocalDateTime getUseStartDateTime() {
        return useStartDateTime;
    }

    public LocalDateTime getUseEndDateTime() {
        return useEndDateTime;
    }

    @Override
    public String toString() {
        return "startDateTime=" + startDateTime +
                ", range=" + range +
                ", endDateTime=" + endDateTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder{

        private LocalDateTime startDateTime;

        private Duration range;

        private S3ScanBucketOption bucketOption;

        private LocalDateTime endDateTime;

        private LocalDateTime useStartDateTime;
        private LocalDateTime useEndDateTime;

        public Builder setStartDateTime(LocalDateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        public Builder setRange(Duration range) {
            this.range = range;
            return this;
        }

        public Builder setEndDateTime(LocalDateTime endDateTime) {
            this.endDateTime = endDateTime;
            return this;
        }

        public Builder setBucketOption(S3ScanBucketOption bucketOption) {
            this.bucketOption = bucketOption;
            return this;
        }

        public ScanOptions build() {
            // Check for bucket-specific time range first
            LocalDateTime bucketStartDateTime = bucketOption.getStartTime();
            LocalDateTime bucketEndDateTime = bucketOption.getEndTime();
            Duration bucketRange = bucketOption.getRange();
            int nonNullCount = (Objects.isNull(bucketStartDateTime) ? 0 : 1) + (Objects.isNull(bucketRange) ? 0 : 1)
                    + (Objects.isNull(bucketEndDateTime) ? 0 : 1);
            if (nonNullCount == 1 || nonNullCount == 3) {
                scanRangeDateValidationError(bucketOption.getName());
            } else if (nonNullCount == 2) {
                if (Objects.nonNull(bucketStartDateTime) && Objects.nonNull(bucketEndDateTime)) {
                    this.useStartDateTime = bucketStartDateTime;
                    this.useEndDateTime = bucketEndDateTime;
                } else if (Objects.nonNull(bucketStartDateTime)) {
                    this.useStartDateTime = bucketStartDateTime;
                    this.useEndDateTime = bucketStartDateTime.plus(bucketRange);
                } else {
                    this.useStartDateTime = bucketEndDateTime.minus(bucketRange);
                    this.useEndDateTime = bucketEndDateTime;
                }
                return new ScanOptions(this);
            }

            // Bucket-specific time range is not configured, use global time range
            nonNullCount = (Objects.isNull(startDateTime) ? 0 : 1) + (Objects.isNull(range) ? 0 : 1)
                    + (Objects.isNull(endDateTime) ? 0 : 1);
            if (nonNullCount == 1 || nonNullCount == 3) {
                scanRangeDateValidationError(null);
            }

            if (Objects.nonNull(startDateTime) && Objects.nonNull(endDateTime)){
                this.useStartDateTime = startDateTime;
                this.useEndDateTime = endDateTime;
            } else if (Objects.nonNull(endDateTime) && Objects.nonNull(range)) {
                this.useStartDateTime = endDateTime.minus(range);
                this.useEndDateTime = endDateTime;
            } else if (Objects.nonNull(startDateTime) && Objects.nonNull(range)) {
                this.useStartDateTime = startDateTime;
                this.useEndDateTime = startDateTime.plus(range);
            }

            return new ScanOptions(this);
        }
        private void scanRangeDateValidationError(String bucketName){
            String message;
            if (Objects.nonNull(bucketName)) {
                message = "To set a time range for the bucket with name " + bucketName +
                        ", specify any two configurations from start_time, end_time and range";
            } else {
                message = "To set a time range for all buckets, specify any two configurations from start_time, end_time and range";
            }
            throw new IllegalArgumentException(message);
        }

        @Override
        public String toString() {
            return "startDateTime=" + startDateTime +
                    ", range=" + range +
                    ", endDateTime=" + endDateTime;
        }
    }
}