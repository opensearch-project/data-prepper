/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;

public class S3SelectOptions {
	
	@JsonProperty("query_statement")
    @NotBlank(message = "query statement cannot be null or empty")
    private String queryStatement;
	
	@JsonProperty("data_serialization_format")
    @NotBlank(message = "data serialization format cannot be null or empty")
    private S3SelectSerializationFormatOption format;

	public String getQueryStatement() {
		return queryStatement;
	}

	public S3SelectSerializationFormatOption getS3SelectSerializationFormatOption() {
		return format;
	}
}
