/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3EnrichKeyPathOption {
    @JsonProperty("include_prefix")
    private String s3scanIncludePrefixOption;
    public String getS3scanIncludePrefixOption() {
        return s3scanIncludePrefixOption;
    }
}
