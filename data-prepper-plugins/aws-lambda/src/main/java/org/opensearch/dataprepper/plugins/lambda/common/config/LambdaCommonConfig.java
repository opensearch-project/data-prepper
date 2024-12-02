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
import java.time.Duration;
import lombok.Getter;
import org.opensearch.dataprepper.model.configuration.PluginModel;

@Getter

public abstract class LambdaCommonConfig {

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

  @JsonPropertyDescription("invocation type defines the way we want to call lambda function")
  @JsonProperty("invocation_type")
  private InvocationType invocationType = InvocationType.REQUEST_RESPONSE;

  @JsonPropertyDescription("Client options")
  @JsonProperty("client")
  private ClientOptions clientOptions = new ClientOptions();

  @JsonPropertyDescription("Batch options")
  @JsonProperty("batch")
  private BatchOptions batchOptions = new BatchOptions();

  @JsonPropertyDescription("Codec configuration for parsing Lambda responses")
  @JsonProperty("response_codec")
  @Valid
  private PluginModel responseCodecConfig;

  public abstract InvocationType getInvocationType();

}
