/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExportConfig {

    @JsonProperty("kms_key_id")
    private String kmsKeyId;

    public String getKmsKeyId() {
        return kmsKeyId;
    }
}
