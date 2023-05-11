/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

class TopicsConfigTest {

    @Mock
    TopicsConfig topicsConfig;

    TopicConfig topicConfig;

    @BeforeEach
    void setUp() throws IOException {
        topicsConfig = new TopicsConfig();
        Yaml yaml = new Yaml();
        FileReader reader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(reader);
        ObjectMapper mapper = new ObjectMapper();
        if(data instanceof Map){
            Map<Object, Object> propertyMap = (Map<Object, Object>) data;
            Map<Object, Object> nestedMap = new HashMap<>();
            iterateMap(propertyMap, nestedMap);
            List<TopicConfig> topicConfigList = (List<TopicConfig>) nestedMap.get("topics");
            mapper.registerModule(new JavaTimeModule());
            topicsConfig = mapper.convertValue(topicConfigList.get(0), TopicsConfig.class);
            topicConfig = topicsConfig.getTopic();
        }
    }

    private static Map<Object, Object> iterateMap(Map<Object, Object> map, Map<Object, Object> nestedMap) {
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            String key = (String) entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                iterateMap((Map<Object, Object>) value, nestedMap);
            } else {
                nestedMap.put(key, value);
            }
        }
        return nestedMap;
    }

    @Test
    void testConfigValues() {
        assertEquals(topicsConfig.getTopic(), topicConfig);

    }
}
