/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.sns;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.sink.sns.configuration.AwsAuthenticationOptions;

import java.util.Map;
import java.util.Objects;

/**
 * sns sink configuration class contains properties, used to read yaml configuration.
 */
public class SnsSinkConfig {

    private static final int DEFAULT_CONNECTION_RETRIES = 5;

    private static final int DEFAULT_BATCH_SIZE = 10;

    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    public static final String STS_REGION = "region";

    public static final String STS_ROLE_ARN = "sts_role_arn";

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("topic_arn")
    @NotNull
    private String topicArn;

    @JsonProperty("message_group_id")
    private String messageGroupId;

    @JsonProperty("codec")
    @NotNull
    private PluginModel codec;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("dlq_file")
    private String dlqFile;

    @JsonProperty("batch_size")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @JsonProperty("message_deduplication_id")
    private String messageDeduplicationId;

    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

    public String getMessageDeduplicationId() {
        return messageDeduplicationId;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public PluginModel getDlq() {
        return dlq;
    }

    public String getDlqFile() {
        return dlqFile;
    }

    /**
     * Aws Authentication configuration Options.
     * @return aws authentication options.
     */
    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public String getTopicArn() {
        return topicArn;
    }

    public String getMessageGroupId() {
        return messageGroupId;
    }

    /**
     * Sink codec configuration Options.
     * @return  codec plugin model.
     */
    public PluginModel getCodec() {
        return codec;
    }

    /**
     * SNS client connection retries configuration Options.
     * @return max connection retries value.
     */
    public int getMaxConnectionRetries() {
        return maxConnectionRetries;
    }

    /**
     * SNS object upload retries configuration Options.
     * @return maximum upload retries value.
     */
    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }

    public String getDlqStsRoleARN(){
        return Objects.nonNull(getDlqPluginSetting().get(STS_ROLE_ARN)) ?
                    String.valueOf(getDlqPluginSetting().get(STS_ROLE_ARN)) :
                    awsAuthenticationOptions.getAwsStsRoleArn();
    }

    public String getDlqStsRegion(){
        return Objects.nonNull(getDlqPluginSetting().get(STS_REGION)) ?
                    String.valueOf(getDlqPluginSetting().get(STS_REGION)) :
                    awsAuthenticationOptions.getAwsRegion().toString();
    }

    public  Map<String, Object> getDlqPluginSetting(){
        return dlq != null ? dlq.getPluginSettings() : Map.of();
    }
}