/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Class consists the scan related properties.
 */
public class ScanOptions {
    private static final Logger LOG = LoggerFactory.getLogger(ScanOptions.class);
    private final LocalDateTime startDateTime;

    private final Duration range;

    private final S3ScanBucketOption bucketOption;

    private final LocalDateTime endDateTime;

    private final LocalDateTime useStartDateTime;

    private final LocalDateTime useEndDateTime;

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
            long globalLevelNonNullCount = Stream.of(startDateTime, endDateTime, range)
                    .filter(Objects::nonNull)
                    .count();

            long originalBucketLevelNonNullCount = Stream.of(
                            bucketOption.getStartTime(), bucketOption.getEndTime(), bucketOption.getRange())
                    .filter(Objects::nonNull)
                    .count();

            if (originalBucketLevelNonNullCount != 0) {
                setDateTimeToUse(bucketOption.getStartTime(), bucketOption.getEndTime(), bucketOption.getRange());
            } else if (globalLevelNonNullCount != 0) {
                setDateTimeToUse(startDateTime, endDateTime, range);
            }

            return new ScanOptions(this);
        }

        private void setDateTimeToUse(LocalDateTime bucketStartDateTime, LocalDateTime bucketEndDateTime, Duration bucketRange) {
            if (Objects.nonNull(bucketStartDateTime) && Objects.nonNull(bucketEndDateTime)) {
                this.useStartDateTime = bucketStartDateTime;
                this.useEndDateTime = bucketEndDateTime;
                LOG.info("Scanning objects modified from {} to {} from bucket: {}", useStartDateTime, useEndDateTime, bucketOption.getName());
            } else if (Objects.nonNull(bucketStartDateTime)) {
                this.useStartDateTime = bucketStartDateTime;
                LOG.info("Scanning objects modified after {} from bucket: {}", useStartDateTime, bucketOption.getName());
            } else if (Objects.nonNull(bucketEndDateTime)) {
                this.useEndDateTime = bucketEndDateTime;
                LOG.info("Scanning objects modified before {} from bucket: {}", useEndDateTime, bucketOption.getName());
            } else if (Objects.nonNull(bucketRange)) {
                this.useEndDateTime = LocalDateTime.now();
                this.useStartDateTime = this.useEndDateTime.minus(bucketRange);
                LOG.info("Scanning objects modified from {} to {} from bucket: {}", useStartDateTime, useEndDateTime, bucketOption.getName());
            } else {
                LOG.info("Scanning all objects from bucket: {}", bucketOption.getName());
            }
        }

        @Override
        public String toString() {
            return "startDateTime=" + startDateTime +
                    ", range=" + range +
                    ", endDateTime=" + endDateTime;
        }
    }
}