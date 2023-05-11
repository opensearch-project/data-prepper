package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

class TopicConfigTest {

    @Mock
    TopicConfig topicConfig;

    private String topicName;

    private ConsumerConfigs consumerConfigs;

    private String authType;

    private SchemaConfig schemaConfig;

    @BeforeEach
    void setUp() throws IOException {
        topicConfig = new TopicConfig();
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader reader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(reader);
        ObjectMapper mapper = new ObjectMapper();
        if(data instanceof Map){
            Map<Object, Object> propertyMap = (Map<Object, Object>) data;
            Map<Object, Object> nestedMap = new HashMap<>();
            iterateMap(propertyMap, nestedMap);
            //keyDeserializer = ;
            List<TopicConfig> topicConfigList = (List<TopicConfig>) nestedMap.get("topics");
            mapper.registerModule(new JavaTimeModule());
            TopicsConfig topicsConfig = mapper.convertValue(topicConfigList.get(0), TopicsConfig.class);
            topicConfig = topicsConfig.getTopic();
            topicName = topicConfig.getName();
            consumerConfigs = topicsConfig.getTopic().getConsumerGroupConfig();
            authType = topicConfig.getAuthType();
            schemaConfig = topicConfig.getSchemaConfig();
            topicsConfig.setTopics(topicConfig);

        }
    }

    private static Map<Object, Object> iterateMap(Map<Object, Object> map, Map<Object, Object> nestedMap) {
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            String key = (String) entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                iterateMap((Map<Object, Object>) value, nestedMap); // call recursively for nested maps
            } else {
                nestedMap.put(key, value);
            }
        }
        return nestedMap;
    }

    @Test
    void testConfigValues() {
        assertEquals(topicConfig.getName(), topicName);
        assertEquals(topicConfig.getConsumerGroupConfig(), consumerConfigs);
        assertEquals(topicConfig.getAuthType(), authType);
        assertEquals(topicConfig.getSchemaConfig(), schemaConfig);

    }
}
