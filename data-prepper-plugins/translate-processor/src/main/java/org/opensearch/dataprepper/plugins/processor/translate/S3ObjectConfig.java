/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;

public class S3ObjectConfig {

    @JsonProperty("bucket")
    @JsonPropertyDescription("The Amazon S3 bucket name.")
    @NotNull
    private String bucket;

    @JsonProperty("region")
    @JsonPropertyDescription("The AWS Region to use for credentials.")
    @NotNull
    private String region;

    @JsonProperty("sts_role_arn")
    @JsonPropertyDescription("The AWS Security Token Service (AWS STS) role to assume for requests to Amazon S3.")
    @NotNull
    private String stsRoleArn;

    public String getBucket(){ return bucket; }

    public String getRegion(){ return region; }

    public String getStsRoleArn(){ return stsRoleArn; }


}
