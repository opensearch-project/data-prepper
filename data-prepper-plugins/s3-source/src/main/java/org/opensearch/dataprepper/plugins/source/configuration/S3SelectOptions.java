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

	public static final String DEFAULT_EXPRESSION_TYPE = "SQL";
	public static final String DEFAULT_COMPRESSION_TYPE = "none";
	@JsonProperty("expression")
	@NotBlank(message = "expression cannot be null or empty")
	private String expression;

	@JsonProperty("expression_type")
	@NotBlank(message = "expression_type cannot be null or empty")
	private String expressionType = DEFAULT_EXPRESSION_TYPE;

	@JsonProperty("input_serialization")
	@NotBlank(message = "input serialization format cannot be null or empty")
	private S3SelectSerializationFormatOption s3SelectSerializationFormatOption;

	@JsonProperty("compression_type")
	private String compressionType = DEFAULT_COMPRESSION_TYPE;

	@JsonProperty("csv")
	private S3SelectCSVOption s3SelectCSVOption = new S3SelectCSVOption();

	@JsonProperty("json")
	private S3SelectJsonOption s3SelectJsonOption = new S3SelectJsonOption();

	public String getExpression() {
		return expression;
	}

	public S3SelectSerializationFormatOption getS3SelectSerializationFormatOption() {
		return s3SelectSerializationFormatOption;
	}

	public String getExpressionType() {
		return expressionType;
	}

	public String getCompressionType() {
		return compressionType;
	}

	public S3SelectCSVOption getS3SelectCSVOption() {
		return s3SelectCSVOption;
	}

	public S3SelectJsonOption getS3SelectJsonOption() {
		return s3SelectJsonOption;
	}
}
