/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.api;

import software.amazon.awssdk.regions.Region;

import java.util.Map;
import java.util.Optional;

public class AwsCredentialsOptions {
    private final String stsRoleArn;
    private final Region region;
    private final Map<String, String> stsHeaderOverrides;

    private AwsCredentialsOptions(final Builder builder) {
        this.stsRoleArn = builder.stsRoleArn;
        this.region = builder.region;
        this.stsHeaderOverrides = builder.stsHeaderOverrides;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<String> getStsRoleArn() {
        return Optional.ofNullable(stsRoleArn);
    }

    public Optional<Region> getRegion() {
        return Optional.ofNullable(region);
    }

    public Map<String, String> getStsHeaderOverrides() {
        return stsHeaderOverrides;
    }

    public static class Builder {
        private String stsRoleArn;
        private Region region;
        private Map<String, String> stsHeaderOverrides;

        public Builder withStsRoleArn(String stsRoleArn) {
            this.stsRoleArn = stsRoleArn;
            return this;
        }

        public Builder withRegion(Region region) {
            this.region = region;
            return this;
        }

        public Builder withRegion(String region) {
            this.region = Region.of(region);
            return this;
        }

        public Builder withStsHeaderOverrides(Map<String, String> stsHeaderOverrides) {
            this.stsHeaderOverrides = stsHeaderOverrides;
            return this;
        }

        public AwsCredentialsOptions build() {
            return new AwsCredentialsOptions(this);
        }
    }
}
