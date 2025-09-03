/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_merge.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.plugins.ml_merge.common.config.AwsAuthenticationOptions;

import java.util.Collections;
import java.util.List;

@Getter
@JsonPropertyOrder
@JsonClassDescription("The <code>ml</code> processor enables invocation of the ml-commons plugin in OpenSearch service within your pipeline in order to process events. " +
        "It supports both synchronous and asynchronous invocations based on your use case.")
public class MLMergeProcessorConfig {
    private static final int DEFAULT_MAX_BATCH_SIZE = 100;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonPropertyDescription("defines the source input to be merged")
    @JsonProperty("source_input")
    private String sourceInput;

    @JsonPropertyDescription("defines the unique key identifier to match the input and output")
    @JsonProperty("match_key")
    private String matchKey;

    @JsonPropertyDescription("Specifies whether to join all the fields in source_input with the data in the pipeline")
    @JsonProperty("join_source")
    private boolean joinSource = false;

    @JsonPropertyDescription("Suffix to identify result files")
    @JsonProperty("result_file_suffix")
    private String resultFileSuffix = ".out";

    @JsonPropertyDescription("Defines a condition for event to use this processor.")
    @ExampleValues({
            @ExampleValues.Example(value = "/some_key == null", description = "The processor will only run on events where this condition evaluates to true.")
    })
    @JsonProperty("merge_when")
    private String whenCondition;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription(
            "A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when ml_merge processor fails to merge "
                    +
                    "or exception occurs. This tag may be used in conditional expressions in " +
                    "other parts of the configuration.")
    private List<String> tagsOnFailure = Collections.emptyList();

    public String getWhenCondition() {
        return whenCondition;
    }

    public List<String> getTagsOnFailure() { return tagsOnFailure; }
}
