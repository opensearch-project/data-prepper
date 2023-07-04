/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

class AwsDlqConfigTest {


	AwsDLQConfig awsDLQConfig;

	@BeforeEach
	void setUp() throws IOException {
		//Added to load Yaml file - Start
		Yaml yaml = new Yaml();
		FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines-sink.yaml").getFile());
		Object data = yaml.load(fileReader);
		if(data instanceof Map){
			Map<String, Object> propertyMap = (Map<String, Object>) data;
			Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
			Map<String, Object> sinkeMap = (Map<String, Object>) logPipelineMap.get("sink");
			Map<String, Object> kafkaConfigMap = (Map<String, Object>) sinkeMap.get("kafka-sink");
			ObjectMapper mapper = new ObjectMapper();
			mapper.registerModule(new JavaTimeModule());
			String json = mapper.writeValueAsString(kafkaConfigMap);
			Reader reader = new StringReader(json);
			awsDLQConfig = mapper.readValue(reader, KafkaSinkConfig.class).getDlqConfig();
		}
	}
	@Test
	void test_aws_props(){
		assertThat(awsDLQConfig.getBucket(), notNullValue());
		assertThat(awsDLQConfig.getRegion(), notNullValue());
		assertThat(awsDLQConfig.getRoleArn(), notNullValue());

	}


}
