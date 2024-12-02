/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Collections;
import java.util.List;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;

public class LambdaProcessorConfig extends LambdaCommonConfig {

  @JsonPropertyDescription("invocation type defines the way we want to call lambda function")
  @JsonProperty("invocation_type")
  private InvocationType invocationType = InvocationType.REQUEST_RESPONSE;

  @JsonPropertyDescription("Defines the way Data Prepper treats the response from Lambda")
  @JsonProperty("response_events_match")
  private boolean responseEventsMatch = false;

  @JsonPropertyDescription("defines a condition for event to use this processor")
  @JsonProperty("lambda_when")
  private String whenCondition;

  @JsonProperty("tags_on_failure")
  @JsonPropertyDescription(
      "A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when lambda fails to "
          +
          "or exception occurs. This tag may be used in conditional expressions in " +
          "other parts of the configuration")
  private List<String> tagsOnFailure = Collections.emptyList();

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
