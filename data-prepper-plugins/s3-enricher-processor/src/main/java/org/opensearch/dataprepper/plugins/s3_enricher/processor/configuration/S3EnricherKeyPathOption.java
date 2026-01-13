/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3EnricherKeyPathOption {
    @JsonProperty("include_prefix")
    private String s3scanIncludePrefixOption;
    public String getS3scanIncludePrefixOption() {
        return s3scanIncludePrefixOption;
    }
}
