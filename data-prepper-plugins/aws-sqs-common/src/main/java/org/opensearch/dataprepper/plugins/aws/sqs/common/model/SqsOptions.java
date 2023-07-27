/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common.model;

import java.time.Duration;

public class SqsOptions {
    private final String sqsUrl;
    private final Integer maximumMessages;
    private final Duration pollDelay;
    private final Duration visibilityTimeout;
    private final Duration waitTime;

    public SqsOptions(final Builder builder) {
        this.sqsUrl = builder.sqsUrl;
        this.maximumMessages = builder.maximumMessages;
        this.pollDelay = builder.pollDelay;
        this.visibilityTimeout = builder.visibilityTimeout;
        this.waitTime = builder.waitTime;
    }

    public String getSqsUrl() {
        return sqsUrl;
    }

    public Integer getMaximumMessages() {
        return maximumMessages;
    }

    public Duration getPollDelay() {
        return pollDelay;
    }

    public Duration getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public Duration getWaitTime() {
        return waitTime;
    }

    public static class Builder{

        private String sqsUrl;

        private Duration visibilityTimeout;

        private Duration waitTime;

        private Integer maximumMessages;

        private Duration pollDelay;


        public Builder setSqsUrl(final String sqsUrl) {
            this.sqsUrl = sqsUrl;
            return this;
        }

        public Builder setMaximumMessages(final Integer maximumMessages) {
            this.maximumMessages = maximumMessages;
            return this;
        }

        public Builder setPollDelay(final Duration pollDelay) {
            this.pollDelay = pollDelay;
            return this;
        }

        public Builder setVisibilityTimeout(Duration visibilityTimeout) {
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        public Builder setWaitTime(Duration waitTime) {
            this.waitTime = waitTime;
            return this;
        }

        public SqsOptions build() {
            return new SqsOptions(this);
        }


    }
}
