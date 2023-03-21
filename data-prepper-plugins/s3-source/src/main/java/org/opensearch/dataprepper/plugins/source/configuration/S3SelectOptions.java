/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;

/**
 * Class consists the s3 select options.
 */
public class S3SelectOptions {
	static final String DEFAULT_CSV_HEADER = "USE";
	
	@JsonProperty("query_statement")
    @NotBlank(message = "query statement cannot be null or empty")
    private String queryStatement;
	
	@JsonProperty("data_serialization_format")
    @NotBlank(message = "data serialization format cannot be null or empty")
    private S3SelectSerializationFormatOption s3SelectSerializationFormatOption;
	@JsonProperty("csv_file_header")
	private String csvFileHeaderInfo = DEFAULT_CSV_HEADER;

	public String getQueryStatement() {
		return queryStatement;
	}

	public S3SelectSerializationFormatOption getS3SelectSerializationFormatOption() {
		return s3SelectSerializationFormatOption;
	}

	public String getCsvFileHeaderInfo() {
		return csvFileHeaderInfo;
	}
}
