/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.opensearch.dataprepper.aws.validator.AwsAccountId;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration.S3EnricherBucketOption;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
@JsonPropertyOrder
@JsonClassDescription("The <code>s3_enricher</code> processor enriches your data from a S3 source")
public class S3EnricherProcessorConfig {
    private static final int DEFAULT_ENRICHER_SIZE_LIMIT = 100;
    private static final int DEFAULT_CACHE_SIZE_LIMIT = 100000;
    private static final int DEFAULT_CACHE_EXPIRE_AFTER_ACCESS_LIMIT = 10;

    @JsonProperty("bucket")
    @Valid
    private S3EnricherBucketOption s3EnricherBucketOption;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("disable_bucket_ownership_validation")
    private boolean disableBucketOwnershipValidation = false;

    @JsonProperty("bucket_owners")
    private Map<String, @AwsAccountId String> bucketOwners;

    @JsonProperty("default_bucket_owner")
    @AwsAccountId
    private String defaultBucketOwner;

    @JsonProperty("codec")
    @NotNull
    private PluginModel codec;

    @JsonProperty("compression")
    private CompressionOption compression = CompressionOption.NONE;

    @JsonProperty(value = "s3_object_size_limit_mb", defaultValue="100")
    @Min(0)
    @Max(300)
    private int enricherSizeLimit = DEFAULT_ENRICHER_SIZE_LIMIT;

    @JsonProperty(value = "cache_max_size", defaultValue="100000")
    @Min(0)
    @Max(300000)
    private int cacheSizeLimit = DEFAULT_CACHE_SIZE_LIMIT;

    @JsonProperty(value = "cache_ttl_minutes", defaultValue="10")
    @Min(0)
    @Max(30)
    private int cacheExpirationMinutes = DEFAULT_CACHE_EXPIRE_AFTER_ACCESS_LIMIT;

    @JsonPropertyDescription("defines the key that defines the s3 enricher object base name")
    @JsonProperty("s3_key_path")
    private String enricherKeyField;

    @JsonPropertyDescription("defines the key ")
    @JsonProperty("s3_object_name_pattern")
    private String enricherNamePattern;

    @JsonPropertyDescription("defines the unique key identifier in the events from the pipeline to match the events from S3 enricher source")
    @JsonProperty("correlation_key")
    private String correlationKey;

    @JsonProperty("keys_to_merge")
    @JsonPropertyDescription("A list of keys of the fields to be merged.")
    private List<EventKey> mergeKeys;

    @JsonPropertyDescription("Defines a condition for event to use this processor.")
    @ExampleValues({
            @ExampleValues.Example(value = "/some_key == null", description = "The processor will only run on events where this condition evaluates to true.")
    })
    @JsonProperty("enrich_when")
    private String whenCondition;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription(
            "A <code>List</code> of <code>String</code>s that specifies the tags to be set in the event when ml_merge processor fails to merge "
                    +
                    "or exception occurs. This tag may be used in conditional expressions in " +
                    "other parts of the configuration.")
    private List<String> tagsOnFailure = Collections.emptyList();
}