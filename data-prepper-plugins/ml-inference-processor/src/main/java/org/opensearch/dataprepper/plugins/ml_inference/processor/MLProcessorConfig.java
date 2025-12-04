/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ServiceName;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
@JsonPropertyOrder
@JsonClassDescription("The <code>ml</code> processor enables invocation of the ml-commons plugin in OpenSearch service within your pipeline in order to process events. " +
        "It supports both synchronous and asynchronous invocations based on your use case.")
public class MLProcessorConfig {
    private static final int DEFAULT_MAX_BATCH_SIZE = 100;
    public static final Duration DEFAULT_RETRY_WINDOW = Duration.ofMinutes(10);
    public static final int DEFAULT_RETRY_INTERVAL_SECONDS = 60;    // default retry interval is 1 minute

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

    @JsonProperty("max_batch_size")
    private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;

    @JsonPropertyDescription("The time duration for which the ml_inference processor retains events for retry attempts."
            + "Supports ISO_8601 notation Strings (\"PT20.345S\", \"PT15M\", etc.) as well as simple notation Strings for seconds (\"60s\") and milliseconds (\"1500ms\")")
    @JsonProperty("retry_time_window")
    private Duration retryTimeWindow = DEFAULT_RETRY_WINDOW;

    @JsonPropertyDescription("The retry interval for throttled records. " +
            "Supports ISO_8601 duration notation (\"PT1M\", \"PT30S\") and simple notation (\"60s\", \"2m\"). " +
            "Valid range: 3 seconds to 5 minutes. Default is 60 seconds.")
    @ExampleValues({
            @ExampleValues.Example(value = "\"PT1M\"", description = "ISO-8601 format for 1 minute"),
            @ExampleValues.Example(value = "\"60s\"", description = "Simple format for 60 seconds"),
            @ExampleValues.Example(value = "\"2m\"", description = "Simple format for 2 minutes")
    })
    @JsonProperty("retry_interval")
    @DurationMin(seconds = 3)
    @DurationMax(seconds = 300)
    private Duration retryInterval = Duration.ofSeconds(DEFAULT_RETRY_INTERVAL_SECONDS);

    @JsonProperty("dlq")
    private PluginModel dlq;

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

    public PluginModel getDlq() {
        return dlq;
    }

    public Map<String, Object> getDlqPluginSetting() {
        return dlq != null ? dlq.getPluginSettings() : null;
    }
}
