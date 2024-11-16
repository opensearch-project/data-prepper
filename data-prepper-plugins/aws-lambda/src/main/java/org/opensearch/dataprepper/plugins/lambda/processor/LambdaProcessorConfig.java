/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;

import java.util.Collections;
import java.util.List;

public class LambdaProcessorConfig extends LambdaCommonConfig {

    @JsonPropertyDescription("Defines the way Data Prepper treats the response from Lambda")
    @JsonProperty("response_events_match")
    private boolean responseEventsMatch = false;

    @JsonPropertyDescription("defines a condition for event to use this processor")
    @JsonProperty("lambda_when")
    private String whenCondition;

    @JsonProperty("tags_on_match_failure")
    @JsonPropertyDescription("A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when lambda fails to " +
            "or exception occurs. This tag may be used in conditional expressions in " +
            "other parts of the configuration")
    private List<String> tagsOnMatchFailure = Collections.emptyList();

    public List<String> getTagsOnMatchFailure(){
        return tagsOnMatchFailure;
    }

    public String getWhenCondition() {
        return whenCondition;
    }

    public Boolean getResponseEventsMatch() {
        return responseEventsMatch;
    }

}
