package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PlainTextAuthConfigTest {

    @Mock
    PlainTextAuthConfig plainTextAuthConfig;
    private String username;
    private String password;
    @BeforeEach
    void setUp() throws IOException {
        plainTextAuthConfig = new PlainTextAuthConfig();
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        ObjectMapper mapper = new ObjectMapper();
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            KafkaSourceConfig kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            plainTextAuthConfig = kafkaSourceConfig.getAuthConfig().getSaslAuthConfig().getPlainTextAuthConfig();
            username = plainTextAuthConfig.getUsername();
            password = plainTextAuthConfig.getPassword();
        }
    }

    @Test
    void testConfigValues() {
        assertEquals(plainTextAuthConfig.getUsername(), username);
        assertEquals(plainTextAuthConfig.getPassword(), password);
    }
}
