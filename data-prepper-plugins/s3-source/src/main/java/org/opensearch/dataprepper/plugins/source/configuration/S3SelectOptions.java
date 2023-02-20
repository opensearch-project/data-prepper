package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;

public class S3SelectOptions {
	
	@JsonProperty("query_statement")
    @NotBlank(message = "query statement cannot be null or empty")
    private String queryStatement;
	
	@JsonProperty("data_serialization_format")
    @NotBlank(message = "data serialization format cannot be null or empty")
    private String dataSerializationFormat;

	public String getQueryStatement() {
		return queryStatement;
	}

	public String getDataSerializationFormat() {
		return dataSerializationFormat;
	}
}
