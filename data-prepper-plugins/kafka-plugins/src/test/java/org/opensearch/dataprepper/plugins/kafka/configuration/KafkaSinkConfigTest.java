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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;


class KafkaSinkConfigTest {


	KafkaSinkConfig kafkaSinkConfig;

	List<String> bootstrapServers;

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
			kafkaSinkConfig = mapper.readValue(reader, KafkaSinkConfig.class);
		}
	}

	@Test
	void test_kafka_config_not_null() {
		assertThat(kafkaSinkConfig, notNullValue());
	}

	@Test
	void test_bootStrapServers_not_null(){
		assertThat(kafkaSinkConfig.getBootStrapServers(), notNullValue());
		List<String> servers = kafkaSinkConfig.getBootStrapServers();
		bootstrapServers = servers.stream().
				flatMap(str -> Arrays.stream(str.split(","))).
				map(String::trim).
				collect(Collectors.toList());
		assertThat(bootstrapServers.size(), equalTo(1));
		assertThat(bootstrapServers, hasItem("localhost:29092"));
	}

	@Test
	void test_topics_not_null(){
		assertThat(kafkaSinkConfig.getTopics(), notNullValue());
	}
	@Test
	void test_schema_not_null(){
		assertThat(kafkaSinkConfig.getSchemaConfig(), notNullValue());
	}
	@Test
	void test_authentication_not_null(){
		assertThat(kafkaSinkConfig.getAuthConfig(), notNullValue());
	}
	@Test
	void test_batch_size_not_null(){
		assertThat(kafkaSinkConfig.getBatchSize(), notNullValue());
	}
	@Test
	void test_compression_type_not_null(){
		assertThat(kafkaSinkConfig.getCompressionType(), notNullValue());
	}
	@Test
	void test_acks_null(){
		assertThat(kafkaSinkConfig.getAcks(), notNullValue());
	}

	@Test
	void test_dlq_config_null(){
		assertThat(kafkaSinkConfig.getDlqConfig(), notNullValue());
	}

	@Test
	void test_thread_wait_time_null(){
		assertThat(kafkaSinkConfig.getThreadWaitTime(), notNullValue());
	}

}
