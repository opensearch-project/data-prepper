package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.plugins.sink.configuration.SinkAwsAuthenticationOptions;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public class S3SinkConfig {

	@JsonProperty("sink_aws")
	@NotNull
	@Valid
	private SinkAwsAuthenticationOptions sinkAwsAuthenticationOptions;

	public SinkAwsAuthenticationOptions getSinkAwsAuthenticationOptions() {
		return sinkAwsAuthenticationOptions;
	}

}
