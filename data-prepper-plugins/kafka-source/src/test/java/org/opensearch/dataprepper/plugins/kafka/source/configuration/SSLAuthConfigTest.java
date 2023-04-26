package org.opensearch.dataprepper.plugins.kafka.source.configuration;

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

class SSLAuthConfigTest {
    @Mock
    SSLAuthConfig sslAuthConfig;
    private String truststoreLocation;
    private String truststorePassword;
    private String keystoreLocation;
    private String keystorePassword;
    private String keyPassword;

    @BeforeEach
    void setUp() throws IOException {
        sslAuthConfig = new SSLAuthConfig();
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
            TopicsConfig topicsConfig = mapper.convertValue(topicConfigList.get(0), TopicsConfig.class);
            TopicConfig topicConfig = topicsConfig.getTopic();
            sslAuthConfig = topicConfig.getSslAuthConfig();
            truststoreLocation = sslAuthConfig.getSslTruststoreLocation();
            truststorePassword = sslAuthConfig.getSslTruststorePassword();
            keystoreLocation = sslAuthConfig.getSslKeystoreLocation();
            keystorePassword = sslAuthConfig.getSslKeystorePassword();
            keyPassword = sslAuthConfig.getSslKeyPassword();
        }
    }
    @Test
    void testConfigValues() {
        assertEquals(sslAuthConfig.getSslTruststoreLocation(), truststoreLocation);
        assertEquals(sslAuthConfig.getSslTruststorePassword(), truststorePassword);
        assertEquals(sslAuthConfig.getSslKeystoreLocation(), keystoreLocation);
        assertEquals(sslAuthConfig.getSslKeystorePassword(), keystorePassword);
        assertEquals(sslAuthConfig.getSslKeyPassword(), keyPassword);

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

}
