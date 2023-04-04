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

	static final String DEFAULT_EXPRESSION_TYPE = "SQL";
	@JsonProperty("expression")
	@NotBlank(message = "expression cannot be null or empty")
	private String expression;

	@JsonProperty("expression_type")
	@NotBlank(message = "expression_type cannot be null or empty")
	private String expressionType = DEFAULT_EXPRESSION_TYPE;

	@JsonProperty("input_serialization")
	@NotBlank(message = "input serialization format cannot be null or empty")
	private S3SelectSerializationFormatOption s3SelectSerializationFormatOption;

	@JsonProperty("csv_file_header")
	private String csvFileHeaderInfo = DEFAULT_CSV_HEADER;

	public String getExpression() {
		return expression;
	}

	public S3SelectSerializationFormatOption getS3SelectSerializationFormatOption() {
		return s3SelectSerializationFormatOption;
	}
	
	public String getCsvFileHeaderInfo() {
		return csvFileHeaderInfo;
	}
}
