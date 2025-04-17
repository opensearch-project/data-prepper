/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ServiceName;

import java.util.Collections;
import java.util.List;

@Getter
@JsonPropertyOrder
@JsonClassDescription("The <code>ml</code> processor enables invocation of the ml-commons plugin in OpenSearch service within your pipeline in order to process events. " +
        "It supports both synchronous and asynchronous invocations based on your use case.")
public class MLProcessorConfig {

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonPropertyDescription("action type defines the way we want to invoke ml-commons in the predict API")
    @JsonProperty("action_type")
    private ActionType actionType = ActionType.BATCH_PREDICT;

    @JsonPropertyDescription("AI service hosting the remote model for ML Commons predictions")
    @JsonProperty("service_name")
    private ServiceName serviceName = ServiceName.SAGEMAKER;

    @JsonPropertyDescription("defines the OpenSearch host url to be invoked")
    @JsonProperty("host")
    private String hostUrl;

    @JsonPropertyDescription("defines the model id to be invoked in ml-commons")
    @JsonProperty("model_id")
    private String modelId;

    @JsonPropertyDescription("defines the S3 location to write the offline model responses to")
    @JsonProperty("output_path")
    private String outputPath;

    @JsonProperty("aws_sigv4")
    private boolean awsSigv4;

    @JsonProperty("input_key")
    @EventKeyConfiguration({EventKeyFactory.EventAction.GET})
    private EventKey inputKey;

    @JsonPropertyDescription("Defines a condition for event to use this processor.")
    @ExampleValues({
            @ExampleValues.Example(value = "/some_key == null", description = "The processor will only run on events where this condition evaluates to true.")
    })
    @JsonProperty("ml_when")
    private String whenCondition;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription(
            "A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when ml_inference processor fails to create jobs "
                    +
                    "or exception occurs. This tag may be used in conditional expressions in " +
                    "other parts of the configuration.")
    private List<String> tagsOnFailure = Collections.emptyList();

    public ActionType getActionType() {
        return actionType;
    }

    public String getModelId() { return modelId; }

    public String getHostUrl() { return hostUrl; }

    public EventKey getInputKey() { return inputKey; }

    public String getWhenCondition() {
        return whenCondition;
    }

    public List<String> getTagsOnFailure() { return tagsOnFailure; }
}
