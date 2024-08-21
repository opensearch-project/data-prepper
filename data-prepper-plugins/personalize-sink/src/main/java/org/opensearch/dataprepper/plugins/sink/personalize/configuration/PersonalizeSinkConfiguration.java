/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.personalize.configuration;

import jakarta.validation.GroupSequence;
import org.opensearch.dataprepper.plugins.sink.personalize.dataset.DatasetTypeOptions;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue;
import software.amazon.awssdk.arns.Arn;

import java.util.List;
import java.util.Optional;

/**
 * personalize sink configuration class contains properties, used to read yaml configuration.
 */
@GroupSequence({PersonalizeSinkConfiguration.class, PersonalizeAdvancedValidation.class})
public class PersonalizeSinkConfiguration {
    private static final int DEFAULT_RETRIES = 10;
    private static final String AWS_PERSONALIZE = "personalize";
    private static final String AWS_PERSONALIZE_DATASET = "dataset";
    private static final List<DatasetTypeOptions> DATASET_ARN_REQUIRED_LIST = List.of(DatasetTypeOptions.USERS, DatasetTypeOptions.ITEMS);

    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("dataset_type")
    @NotNull
    @Valid
    private DatasetTypeOptions datasetType;

    @JsonProperty("dataset_arn")
    private String datasetArn;

    @JsonProperty("tracking_id")
    private String trackingId;

    @JsonProperty("document_root_key")
    private String documentRootKey;

    @JsonProperty("max_retries")
    private int maxRetries = DEFAULT_RETRIES;

    @AssertTrue(message = "A dataset arn is required for items and users datasets.", groups = PersonalizeAdvancedValidation.class)
    boolean isDatasetArnProvidedWhenNeeded() {
        if (DATASET_ARN_REQUIRED_LIST.contains(datasetType)) {
            return datasetArn != null;
        }
        return true;
    }

    @AssertTrue(message = "dataset_arn must be a Personalize Dataset arn", groups = PersonalizeAdvancedValidation.class)
    boolean isValidDatasetArn() {
        if (datasetArn == null) {
            return true;
        }
        final Arn arn = getArn();
        boolean status = true;
        if (!AWS_PERSONALIZE.equals(arn.service())) {
            status = false;
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_PERSONALIZE_DATASET)) {
            status = false;
        }
        return status;
    }

    private Arn getArn() {
        try {
            return Arn.fromString(datasetArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for datasetArn. Check the format of %s", datasetArn), e);
        }
    }

    @AssertTrue(message = "A tracking id is required for interactions dataset.", groups = PersonalizeAdvancedValidation.class)
    boolean isTrackingIdProvidedWhenNeeded() {
        if (DatasetTypeOptions.INTERACTIONS.equals(datasetType)) {
            return trackingId != null;
        }
        return true;
    }

    /**
     * Aws Authentication configuration Options.
     * @return aws authentication options.
     */
    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    /**
     * Dataset type configuration Options.
     * @return dataset type option object.
     */
    public DatasetTypeOptions getDatasetType() {
        return datasetType;
    }

    /**
     * Dataset arn for Personalize Dataset.
     * @return dataset arn string.
     */
    public String getDatasetArn() {
        return datasetArn;
    }

    /**
     * Tracking id for Personalize Event Tracker.
     * @return tracking id string.
     */
    public String getTrackingId() {
        return trackingId;
    }

    /**
     * Tracking id for Personalize Event Tracker.
     * @return document root key string.
     */
    public String getDocumentRootKey() {
        return documentRootKey;
    }

    /**
     * Personalize client retries configuration Options.
     * @return maximum retries value.
     */
    public int getMaxRetries() {
        return maxRetries;
    }
}