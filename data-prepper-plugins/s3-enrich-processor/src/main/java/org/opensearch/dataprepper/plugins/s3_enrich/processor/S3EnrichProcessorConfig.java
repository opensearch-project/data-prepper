/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.aws.validator.AwsAccountId;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.constraints.ByteCountMax;
import org.opensearch.dataprepper.model.constraints.ByteCountMin;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.s3.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.configuration.S3EnrichBucketOption;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.configuration.S3EnrichKeyPathOption;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@JsonPropertyOrder
@JsonClassDescription("The <code>s3_enricher</code> processor enriches your data from a S3 source")
public class S3EnrichProcessorConfig {
    private static final String DEFAULT_ENRICHER_SIZE_LIMIT = "100mb";
    private static final int DEFAULT_CACHE_SIZE_LIMIT = 100000;
    private static final Duration DEFAULT_CACHE_TTL = Duration.ofMinutes(10);

    @JsonProperty("bucket")
    @Valid
    private S3EnrichBucketOption s3EnrichBucketOption;

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

    @JsonProperty(value = "s3_object_size_limit", defaultValue = DEFAULT_ENRICHER_SIZE_LIMIT)
    @ByteCountMin(DEFAULT_ENRICHER_SIZE_LIMIT)
    @ByteCountMax("300mb")
    private ByteCount enricherSizeLimit = ByteCount.parse(DEFAULT_ENRICHER_SIZE_LIMIT);

    @JsonProperty(value = "cache_max_size", defaultValue="100000")
    @Min(0)
    @Max(300000)
    private int cacheSizeLimit = DEFAULT_CACHE_SIZE_LIMIT;

    @JsonProperty(value = "cache_ttl", defaultValue = "PT10M")
    @JsonPropertyDescription("The TTL for cache entries. Accepts ISO-8601 duration format (e.g., PT10M for 10 minutes, PT1H for 1 hour).")
    private Duration cacheTtl = DEFAULT_CACHE_TTL;

    @JsonPropertyDescription("defines the key that defines the s3 enricher object base name")
    @JsonProperty("s3_key_path")
    private String enricherKeyPath;

    @JsonPropertyDescription("defines the key ")
    @JsonProperty("s3_object_name_pattern")
    private String enricherNamePattern;

    @JsonPropertyDescription("defines the unique key identifier in the events from the pipeline to match the events from S3 enricher source")
    @JsonProperty("correlation_keys")
    @Size(min = 1, max = 1)
    private List<String> correlationKeys;

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

    /**
     * Safely retrieves the S3 scan include prefix from the configuration chain.
     *
     * @return Optional containing the prefix if present, empty Optional otherwise
     */
    public Optional<String> getS3IncludePrefix() {
        return Optional.ofNullable(s3EnrichBucketOption)
                .map(S3EnrichBucketOption::getS3SourceFilter)
                .map(S3EnrichKeyPathOption::getS3scanIncludePrefixOption)
                .filter(prefix -> !prefix.isBlank());
    }
}