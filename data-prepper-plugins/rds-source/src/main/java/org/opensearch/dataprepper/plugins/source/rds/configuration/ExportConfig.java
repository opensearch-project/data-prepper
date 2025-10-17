/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class ExportConfig {

    @JsonProperty("kms_key_id")
    @NotNull
    private String kmsKeyId;

    /**
     * The ARN of the IAM role that will be passed to RDS for export.
     */
    @JsonProperty("iam_role_arn")
    @JsonAlias("export_role_arn")
    @NotNull
    private String iamRoleArn;

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    public String getIamRoleArn() {
        return iamRoleArn;
    }
}
