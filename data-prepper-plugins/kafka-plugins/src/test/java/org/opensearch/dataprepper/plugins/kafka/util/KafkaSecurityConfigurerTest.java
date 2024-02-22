package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class KafkaSecurityConfigurerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSecurityConfigurerTest.class);
    @Test
    public void testSetAuthPropertiesWithSaslPlainCertificate() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-certificate-content.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is("CERTIFICATE_DATA"));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(CustomClientSslEngineFactory.class));
    }

    @Test
    public void testSetAuthPropertiesWithNoAuthSsl() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-no-auth-ssl.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is("SSL"));
        assertThat(props.getProperty("certificateContent"), is("CERTIFICATE_DATA"));
        assertThat(props.get("ssl.engine.factory.class"), is(CustomClientSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesWithNoAuthSslNone() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-no-auth-ssl-none.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is(nullValue()));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesWithNoAuthInsecure() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-auth-insecure.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_PLAINTEXT"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(InsecureSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesAuthSslWithTrustStore() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-truststore.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is("some-file-path"));
        assertThat(props.getProperty("ssl.truststore.password"), is("some-password"));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    private KafkaSourceConfig createKafkaSinkConfig(final String fileName) throws IOException {
        final Yaml yaml = new Yaml();
        final FileReader fileReader = new FileReader(Objects.requireNonNull(getClass().getClassLoader()
                .getResource(fileName)).getFile());
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
