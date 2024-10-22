/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.api;

import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a standard model for requesting AWS credentials.
 */
public class AwsCredentialsOptions {
    private static final AwsCredentialsOptions DEFAULT_OPTIONS = new AwsCredentialsOptions();
    private static final AwsCredentialsOptions DEFAULT_OPTIONS_WITH_DEFAULT_CREDS_PROVIDER =
            AwsCredentialsOptions.builder().withUseDefaultCredentialsProvider(true).build();
    private final String stsRoleArn;
    private final String stsExternalId;
    private final Region region;
    private final Map<String, String> stsHeaderOverrides;
    private final boolean useDefaultCredentialsProvider;

    private AwsCredentialsOptions(final Builder builder) {
        this.stsRoleArn = builder.stsRoleArn;
        this.stsExternalId = builder.stsExternalId;
        this.region = builder.region;
        this.stsHeaderOverrides = builder.stsHeaderOverrides != null ? new HashMap<>(builder.stsHeaderOverrides) : Collections.emptyMap();
        this.useDefaultCredentialsProvider = builder.useDefaultCredentialsProvider;
    }

    private AwsCredentialsOptions() {
        this.stsRoleArn = null;
        this.stsExternalId = null;
        this.region = null;
        this.stsHeaderOverrides = Collections.emptyMap();
        this.useDefaultCredentialsProvider = false;
    }

    /**
     * Constructs a new {@link Builder} to build the credentials
     * options.
     *
     * @return A new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static AwsCredentialsOptions defaultOptions() {
        return DEFAULT_OPTIONS;
    }

    public static AwsCredentialsOptions defaultOptionsWithDefaultCredentialsProvider() {
        return DEFAULT_OPTIONS_WITH_DEFAULT_CREDS_PROVIDER;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }

    public String getStsExternalId() {
        return stsExternalId;
    }

    public Region getRegion() {
        return region;
    }

    public Map<String, String> getStsHeaderOverrides() {
        return stsHeaderOverrides;
    }

    public boolean isUseDefaultCredentialsProvider() {
        return useDefaultCredentialsProvider;
    }

    /**
     * Builder class for {@link AwsCredentialsOptions}.
     */
    public static class Builder {
        private String stsRoleArn;
        private String stsExternalId;
        private Region region;
        private Map<String, String> stsHeaderOverrides = Collections.emptyMap();
        private boolean useDefaultCredentialsProvider = false;

        /**
         * Sets the STS role ARN to use.
         *
         * @param stsRoleArn The STS role ARN
         * @return The {@link Builder} for continuing to build
         */
        public Builder withStsRoleArn(final String stsRoleArn) {
            this.stsRoleArn = stsRoleArn;
            return this;
        }

        public Builder withStsExternalId(final String stsExternalId) {
            this.stsExternalId = stsExternalId;
            return this;
        }
        /**
         * Sets the AWS region using the model class from the AWS SDK.
         *
         * @param region The AWS region
         * @return The {@link Builder} for continuing to build
         */
        public Builder withRegion(final Region region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the AWS region from a string.
         *
         * @param region The AWS region
         * @return The {@link Builder} for continuing to build
         */
        public Builder withRegion(final String region) {
            this.region = Region.of(region);
            return this;
        }

        /**
         * Configures header overrides for requests to STS.
         *
         * @param stsHeaderOverrides A map of STS header overrides
         * @return The {@link Builder} for continuing to build
         */
        public Builder withStsHeaderOverrides(final Map<String, String> stsHeaderOverrides) {
            this.stsHeaderOverrides = stsHeaderOverrides;
            return this;
        }

        /**
         * Configures whether to use default credentials.
         *
         * @param useDefaultCredentialsProvider
         * @return The {@link Builder} for continuing to build
         */
        public Builder withUseDefaultCredentialsProvider(final boolean useDefaultCredentialsProvider) {
            this.useDefaultCredentialsProvider = useDefaultCredentialsProvider;
            return this;
        }

        /**
         * Builds the {@link AwsCredentialsOptions}.
         *
         * @return A new {@link AwsCredentialsOptions}.
         */
        public AwsCredentialsOptions build() {
            return new AwsCredentialsOptions(this);
        }
    }
}
