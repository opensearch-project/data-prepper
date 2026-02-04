/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class SqsSinkConfig {
    public static int DEFAULT_MAX_RETRIES = 3;

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
    @Valid
    private SqsThresholdConfig thresholdConfig = new SqsThresholdConfig();

    @JsonProperty("max_retries")
    private int maxRetries = DEFAULT_MAX_RETRIES;

    @JsonProperty("group_id")
    private String groupId;

    @JsonProperty("deduplication_id")
    private String deDuplicationId;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @AssertTrue(message = "FIFO queues wth dynamic group id or dynamic deduplication id and more than one events per message is not valid OR standard queues do not support groupId or deduplication configuration")
    boolean isValidConfig() {
        String deDupId = getDeDuplicationId();
        String groupId = getGroupId();
        String queueUrl = getQueueUrl();
        boolean isDynamicDeDupId = deDupId != null && deDupId.contains("${");
        boolean isDynamicGroupId = groupId != null && groupId.contains("${");
        boolean isDynamicQueueUrl = queueUrl != null && queueUrl.contains("${");
        if (isDynamicQueueUrl) {
            return true;
        }
        if (getQueueUrl().endsWith(".fifo")) {
            if ((isDynamicGroupId  || isDynamicDeDupId) && thresholdConfig.getMaxEventsPerMessage() > 1) {
                return false;
            } else{
                return true;
            }
        } else {
             return (groupId == null && deDupId == null);
        }
    }

    @AssertTrue(message = "ndjson codec doesn't support max events per message greater than 1")
    boolean isValidCodecConfig() {
        if ((codec != null && codec.getPluginName().equals("ndjson")) && thresholdConfig.getMaxEventsPerMessage() > 1)
            return false;
        return true;
    }
}

