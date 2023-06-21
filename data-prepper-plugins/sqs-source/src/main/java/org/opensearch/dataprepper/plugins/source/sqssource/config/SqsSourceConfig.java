/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;

public class SqsSourceConfig {

    @Valid
    @JsonProperty("queues")
    private QueuesOptions queues;

    @Valid
    @JsonProperty("aws")
    private AwsAuthenticationOptions aws;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    public QueuesOptions getQueues() {
        return queues;
    }

    public AwsAuthenticationOptions getAws() {
        return aws;
    }

    public boolean getAcknowledgements() {
        return acknowledgments;
    }

}
