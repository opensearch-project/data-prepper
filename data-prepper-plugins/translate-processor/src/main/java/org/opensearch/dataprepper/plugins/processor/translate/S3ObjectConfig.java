/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class S3ObjectConfig {

    @JsonProperty("bucket")
    @NotNull
    private String bucket;

    @JsonProperty("region")
    @NotNull
    private String region;

    @JsonProperty("sts_role_arn")
    @NotNull
    private String stsRoleArn;

    public String getBucket(){ return bucket; }

    public String getRegion(){ return region; }

    public String getStsRoleArn(){ return stsRoleArn; }


}
