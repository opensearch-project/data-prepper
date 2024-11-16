/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;

import lombok.Getter;
import java.time.Duration;

@Getter
public class LambdaCommonConfig {
    public static final int DEFAULT_CONNECTION_RETRIES = 3;
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(60);
    public static final String STS_REGION = "region";
    public static final String STS_ROLE_ARN = "sts_role_arn";

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonPropertyDescription("Lambda Function Name")
    @JsonProperty("function_name")
    @NotEmpty
    @Size(min = 3, max = 500, message = "function name length should be at least 3 characters")
    private String functionName;

    @JsonPropertyDescription("Total retries we want before failing")
    @JsonProperty("max_retries")
    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    @JsonPropertyDescription("invocation type defines the way we want to call lambda function")
    @JsonProperty("invocation_type")
    private InvocationType invocationType = InvocationType.REQUEST_RESPONSE;

    @JsonPropertyDescription("sdk timeout defines the time sdk maintains the connection to the client before timing out")
    @JsonProperty("connection_timeout")
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @JsonProperty("batch")
    private BatchOptions batchOptions;

    @JsonPropertyDescription("Codec configuration for parsing Lambda responses")
    @JsonProperty("response_codec")
    @Valid
    private PluginModel responseCodecConfig;

}
