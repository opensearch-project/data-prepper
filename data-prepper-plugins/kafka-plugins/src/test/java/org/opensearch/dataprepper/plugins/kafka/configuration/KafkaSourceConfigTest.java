package org.opensearch.dataprepper.plugins.kafka.configuration;

import static org.junit.Assert.assertEquals;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.yaml.snakeyaml.Yaml;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class KafkaSourceConfigTest {

	@Mock
	KafkaSourceConfig kafkaSourceConfig;

	@Mock
	Properties properties;

	@Mock
    ConsumerConfigs consumerConfigs;

	@Mock
	SchemaConfig schemaConfig;

	List<String> bootstrapServers;

	List<TopicsConfig> topicsConfigs;

	@BeforeEach
	void setUp() throws IOException {
		kafkaSourceConfig = new KafkaSourceConfig();
		consumerConfigs = new ConsumerConfigs();
		schemaConfig = new SchemaConfig();
		//Added to load Yaml file - Start
		Yaml yaml = new Yaml();
		FileReader reader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
		Object data = yaml.load(reader);
		if(data instanceof Map){
			Map<String, Object> propertyMap = (Map<String, Object>) data;
			Map<String, Object> nestedMap = new HashMap<>();
			iterateMap(propertyMap, nestedMap);
			bootstrapServers = (List<String>) nestedMap.get("bootstrap_servers");
			topicsConfigs = (List<TopicsConfig>) nestedMap.get("topics");
			kafkaSourceConfig.setBootStrapServers(bootstrapServers);
			kafkaSourceConfig.setTopics(topicsConfigs);
		}
	}

	private static Map<String, Object> iterateMap(Map<String, Object> map, Map<String, Object> nestedMap) {
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value instanceof Map) {
				iterateMap((Map<String, Object>) value, nestedMap);
			} else {
				nestedMap.put(key, value);
			}
		}
		return nestedMap;
	}
	@Test
	void testConfigValues() {
		assertEquals(kafkaSourceConfig.getBootStrapServers(),
				bootstrapServers);
		assertEquals(kafkaSourceConfig.getTopics(),
				topicsConfigs);

	}

}
