/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.plugins.common.aws.AwsConfig;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class SqsSinkConfig {

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("queue_url")
    @NotNull
    private String queueUrl;

    @JsonProperty("codec")
    @NotNull
    private PluginModel codec;

    @JsonProperty("threshold")
    private SqsThresholdConfig thresholdConfig = new SqsThresholdConfig();

    @JsonProperty("max_retries")
    private int maxRetries;

    @JsonProperty("backoff_max_delay")
    private int backoffMaxDelay;

    @JsonProperty("group_id")
    private String groupId;

    @JsonProperty("dlq")
    private PluginModel dlq;

}

