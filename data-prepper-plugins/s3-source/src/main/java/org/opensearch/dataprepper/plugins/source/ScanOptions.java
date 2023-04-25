/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Class consists the scan related properties.
 */
public class ScanOptions {

    private LocalDateTime startDateTime;

    private Duration range;

    private String bucket;

    private String expression;

    private List<String> includeKeyPaths;

    private List<String> excludeKeyPaths;
    private LocalDateTime endDateTime;

    private LocalDateTime useStartDateTime;

    private LocalDateTime useEndDateTime;

    private ScanOptions(Builder builder){
        this.startDateTime = builder.startDateTime;
        this.range = builder.range;
        this.bucket = builder.bucket;
        this.expression = builder.expression;
        this.includeKeyPaths = builder.includeKeyPaths;
        this.excludeKeyPaths = builder.excludeKeyPaths;
        this.endDateTime = builder.endDateTime;
        this.useStartDateTime = builder.useStartDateTime;
        this.useEndDateTime = builder.useEndDateTime;

    }
    public Duration getRange() {
        return range;
    }

    public String getBucket() {
        return bucket;
    }

    public String getExpression() {
        return expression;
    }

    public List<String> getIncludeKeyPaths() {
        return includeKeyPaths;
    }

    public List<String> getExcludeKeyPaths() {
        return excludeKeyPaths;
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
    public static class Builder{

        private LocalDateTime startDateTime;

        private Duration range;

        private String bucket;

        private String expression;

        private List<String> includeKeyPaths;

        private List<String> excludeKeyPaths;

        private LocalDateTime endDateTime;

        private LocalDateTime useStartDateTime;
        private LocalDateTime useEndDateTime;

        Builder(){

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

        public Builder setExpression(String expression) {
            this.expression = expression;
            return this;
        }
        public Builder setIncludeKeyPaths(List<String> includeKeyPaths) {
            this.includeKeyPaths = includeKeyPaths;
            return this;
        }

        public Builder setExcludeKeyPaths(List<String> excludeKeyPaths) {
            this.excludeKeyPaths = excludeKeyPaths;
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
    }
}