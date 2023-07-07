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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.notNullValue;


class AuthConfigTest {

    @Mock
    AuthConfig authConfig;

    @BeforeEach
    void setUp() throws IOException {
        authConfig = new AuthConfig();
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
            authConfig = kafkaSourceConfig.getAuthConfig();
        }
    }

    @Test
    void testConfig() {
        assertThat(authConfig, notNullValue());
        assertThat(authConfig.getPlainTextAuthConfig(), notNullValue());
        assertThat(authConfig.getPlainTextAuthConfig(), hasProperty("username"));
        assertThat(authConfig.getPlainTextAuthConfig(), hasProperty("password"));
        assertThat(authConfig.getOAuthConfig(), notNullValue());
    }

}