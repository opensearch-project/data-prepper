package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class KafkaSecurityConfigurerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSecurityConfigurerTest.class);
    @Test
    public void testSetAuthProperties() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig();
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        Assertions.assertEquals("PLAIN", props.getProperty("sasl.mechanism"));
        Assertions.assertEquals("true", props.getProperty("enable.ssl.certificate.verification"));
        Assertions.assertEquals("SASL_SSL", props.getProperty("security.protocol"));
        Assertions.assertEquals("CERTIFICATE_DATA", props.getProperty("certificateContent"));
        Assertions.assertEquals(CustomClientSslEngineFactory.class, props.get("ssl.engine.factory.class"));
    }

    private KafkaSourceConfig createKafkaSinkConfig() throws IOException {
        final Yaml yaml = new Yaml();
        final FileReader fileReader = new FileReader(Objects.requireNonNull(getClass().getClassLoader()
                .getResource("kafka-pipeline-sasl-ssl.yaml")).getFile());
        final Map<String, Map<String, Map<String, Map<String, Object>>>> data = yaml.load(fileReader);
        final Map<String, Map<String, Map<String, Object>>> logPipelineMap = data.get("log-pipeline");
        final Map<String, Map<String, Object>> sourceMap = logPipelineMap.get("source");
        final Map<String, Object> kafkaConfigMap = sourceMap.get("kafka");
        final ObjectMapper mapper = new ObjectMapper();
        final String json = mapper.writeValueAsString(kafkaConfigMap);
        final Reader reader = new StringReader(json);
        return mapper.readValue(reader, KafkaSourceConfig.class);
    }
}
