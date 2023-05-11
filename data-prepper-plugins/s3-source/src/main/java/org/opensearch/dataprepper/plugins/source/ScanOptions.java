/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Class consists the scan related properties.
 */
public class ScanOptions {

    private LocalDateTime startDateTime;

    private Duration range;

    private String bucket;

    private LocalDateTime endDateTime;

    private LocalDateTime useStartDateTime;

    private LocalDateTime useEndDateTime;

    private S3ScanKeyPathOption s3ScanKeyPathOption;

    private ScanOptions(Builder builder){
        this.startDateTime = builder.startDateTime;
        this.range = builder.range;
        this.bucket = builder.bucket;
        this.endDateTime = builder.endDateTime;
        this.useStartDateTime = builder.useStartDateTime;
        this.useEndDateTime = builder.useEndDateTime;
        this.s3ScanKeyPathOption = builder.s3ScanKeyPathOption;

    }
    public Duration getRange() {
        return range;
    }

    public String getBucket() {
        return bucket;
    }

    public LocalDateTime getUseStartDateTime() {
        return useStartDateTime;
    }

    public LocalDateTime getUseEndDateTime() {
        return useEndDateTime;
    }

    public S3ScanKeyPathOption getS3ScanKeyPathOption() {
        return s3ScanKeyPathOption;
    }

    @Override
    public String toString() {
        return "startDateTime=" + startDateTime +
                ", range=" + range +
                ", endDateTime=" + endDateTime;
    }
    public static class Builder{

        private LocalDateTime startDateTime;

        private Duration range;

        private String bucket;

        private LocalDateTime endDateTime;

        private LocalDateTime useStartDateTime;
        private LocalDateTime useEndDateTime;

        private S3ScanKeyPathOption s3ScanKeyPathOption;

        Builder(){

        }

        public Builder setS3ScanKeyPathOption(S3ScanKeyPathOption s3ScanKeyPathOption) {
            this.s3ScanKeyPathOption = s3ScanKeyPathOption;
            return this;
        }

        public Builder setStartDateTime(LocalDateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        public Builder setRange(Duration range) {
            this.range = range;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setEndDateTime(LocalDateTime endDateTime) {
            this.endDateTime = endDateTime;
            return this;
        }
        public ScanOptions build() {
            if(Objects.nonNull(startDateTime) && Objects.nonNull(range) && Objects.nonNull(endDateTime))
                scanRangeDateValidationError();

            if(Objects.nonNull(startDateTime) && Objects.nonNull(endDateTime)){
                this.useStartDateTime = startDateTime;
                this.useEndDateTime = endDateTime;
            } else if (Objects.nonNull(endDateTime) && Objects.nonNull(range)) {
                this.useStartDateTime = endDateTime.minus(range);
                this.useEndDateTime = endDateTime;
            } else if(Objects.nonNull(startDateTime) && Objects.nonNull(range)) {
                this.useStartDateTime = startDateTime;
                this.useEndDateTime = startDateTime.plus(range);
            } else
                scanRangeDateValidationError();

            return new ScanOptions(this);
        }
        private void scanRangeDateValidationError(){
            throw new IllegalArgumentException("start_date/range,start_date/end_date,end_date/range any two combinations " +
                    "are required to process scan range");
        }

        @Override
        public String toString() {
            return "startDateTime=" + startDateTime +
                    ", range=" + range +
                    ", endDateTime=" + endDateTime;
        }
    }
}