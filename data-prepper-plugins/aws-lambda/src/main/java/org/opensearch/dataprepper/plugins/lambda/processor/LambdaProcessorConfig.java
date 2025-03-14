/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import java.util.Collections;
import java.util.List;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;

@JsonPropertyOrder
@JsonClassDescription("The <code>aws_lambda</code> processor enables invocation of an AWS Lambda function within your pipeline in order to process events. " +
        "It supports both synchronous and asynchronous invocations based on your use case.")
public class LambdaProcessorConfig extends LambdaCommonConfig {
  static final String DEFAULT_INVOCATION_TYPE = "request-response";

  @JsonPropertyDescription("Specifies the invocation type, either <code>request-response</code> or <code>event</code>. Default is <code>request-response</code>.")
  @JsonProperty(value = "invocation_type", defaultValue = DEFAULT_INVOCATION_TYPE)
  private InvocationType invocationType = InvocationType.REQUEST_RESPONSE;

  @JsonPropertyDescription("Specifies how Data Prepper interprets and processes Lambda function responses. Default is <code>false</code>.")
  @JsonProperty("response_events_match")
  private boolean responseEventsMatch = false;

  @JsonProperty("tags_on_failure")
  @JsonPropertyDescription(
      "A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when lambda fails to "
          +
          "or exception occurs. This tag may be used in conditional expressions in " +
          "other parts of the configuration.")
  private List<String> tagsOnFailure = Collections.emptyList();

  @JsonPropertyDescription("Defines a condition for event to use this processor.")
  @ExampleValues({
          @Example(value = "/some_key == null", description = "The processor will only run on events where this condition evaluates to true.")
  })
  @JsonProperty("lambda_when")
  private String whenCondition;

  public List<String> getTagsOnFailure() {
    return tagsOnFailure;
  }

  public String getWhenCondition() {
    return whenCondition;
  }

  public Boolean getResponseEventsMatch() {
    return responseEventsMatch;
  }

  @Override
  public InvocationType getInvocationType() {
    return invocationType;
  }
}
