/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common.model;

import java.time.Duration;

public class SqsOptions {
    private static final int DEFAULT_MAXIMUM_MESSAGES = 10;
    private static final Duration DEFAULT_POLL_DELAY_SECONDS = Duration.ofSeconds(0);
    private final String sqsUrl;
    private final int maximumMessages;
    private final Duration pollDelay;

    public SqsOptions(Builder builder) {
        this.sqsUrl = builder.sqsUrl;
        this.maximumMessages = builder.maximumMessages;
        this.pollDelay = builder.pollDelay;
    }

    public String getSqsUrl() {
        return sqsUrl;
    }

    public int getMaximumMessages() {
        return maximumMessages;
    }

    public Duration getPollDelay() {
        return pollDelay;
    }

    public static class Builder{

        private String sqsUrl;
        private int maximumMessages = DEFAULT_MAXIMUM_MESSAGES;
        private Duration pollDelay = DEFAULT_POLL_DELAY_SECONDS;

        public Builder setSqsUrl(String sqsUrl) {
            this.sqsUrl = sqsUrl;
            return this;
        }

        public Builder setMaximumMessages(int maximumMessages) {
            this.maximumMessages = maximumMessages;
            return this;
        }

        public Builder setPollDelay(Duration pollDelay) {
            this.pollDelay = pollDelay;
            return this;
        }
        public SqsOptions build() {
            return new SqsOptions(this);
        }


    }
}
